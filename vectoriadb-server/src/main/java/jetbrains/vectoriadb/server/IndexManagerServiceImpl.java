/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.vectoriadb.server;

import com.google.protobuf.Empty;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PreDestroy;
import jetbrains.vectoriadb.index.DataStore;
import jetbrains.vectoriadb.index.Distance;
import jetbrains.vectoriadb.index.IndexBuilder;
import jetbrains.vectoriadb.index.IndexReader;
import jetbrains.vectoriadb.index.diskcache.DiskCache;
import jetbrains.vectoriadb.service.base.IndexManagerGrpc;
import jetbrains.vectoriadb.service.base.IndexManagerOuterClass;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

@GrpcService
public class IndexManagerServiceImpl extends IndexManagerGrpc.IndexManagerImplBase {
    private static final long EIGHT_TB = 8L * 1024 * 1024 * 1024 * 1024;

    public static final String INDEX_DIMENSIONS_PROPERTY = "vectoriadb.index.dimensions";
    public static final String MAX_CONNECTIONS_PER_VERTEX_PROPERTY = "vectoriadb.index.max-connections-per-vertex";
    public static final String MAX_CANDIDATES_RETURNED_PROPERTY = "vectoriadb.index.max-candidates-returned";
    public static final String COMPRESSION_RATIO_PROPERTY = "vectoriadb.index.compression-ratio";
    public static final String DISTANCE_MULTIPLIER_PROPERTY = "vectoriadb.index.distance-multiplier";
    public static final String INDEX_BUILDING_MAX_MEMORY_CONSUMPTION_PROPERTY =
            "vectoriadb.index.building.max-memory-consumption";
    public static final String INDEX_READER_DISK_CACHE_MEMORY_CONSUMPTION =
            "vectoriadb.index.reader.disk-cache-memory-consumption";

    public static final String BASE_PATH_PROPERTY = "vectoriadb.server.base-path";
    public static final String DEFAULT_MODE_PROPERTY = "vectoriadb.server.default-mode";

    public static final String BUILD_MODE = "build";

    public static final String SEARCH_MODE = "search";

    private static final int DEFAULT_DIMENSIONS = 128;
    private static final Logger logger = LoggerFactory.getLogger(IndexManagerServiceImpl.class);

    private final ConcurrentHashMap<String, IndexState> indexStates = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, IndexMetadata> indexMetadatas = new ConcurrentHashMap<>();

    private final ListenerBasedPeriodicProgressTracker progressTracker = new ListenerBasedPeriodicProgressTracker(5);
    private static final int MAXIMUM_UPLOADERS_COUNT = 64;
    private final Set<String> uploadingIndexes = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ReentrantLock uploaderLock = new ReentrantLock();

    private final int dimensions;
    private final int maxConnectionsPerVertex;
    private final int maxCandidatesReturned;
    private final int compressionRatio;
    private final float distanceMultiplier;
    private final long indexBuildingMaxMemoryConsumption;
    private final long diskCacheMemoryConsumption;
    private final Semaphore operationsSemaphore = new Semaphore(Integer.MAX_VALUE);
    private final ReentrantLock modeLock = new ReentrantLock();

    private boolean closed = false;

    private volatile Mode mode;

    private final Path basePath;

    public IndexManagerServiceImpl(Environment environment) {
        dimensions = environment.getProperty(INDEX_DIMENSIONS_PROPERTY, Integer.class, DEFAULT_DIMENSIONS);

        maxConnectionsPerVertex = environment.getProperty(MAX_CONNECTIONS_PER_VERTEX_PROPERTY, Integer.class,
                IndexBuilder.DEFAULT_MAX_CONNECTIONS_PER_VERTEX);
        maxCandidatesReturned = environment.getProperty(MAX_CANDIDATES_RETURNED_PROPERTY, Integer.class,
                IndexBuilder.DEFAULT_MAX_AMOUNT_OF_CANDIDATES);
        compressionRatio = environment.getProperty(COMPRESSION_RATIO_PROPERTY, Integer.class,
                IndexBuilder.DEFAULT_COMPRESSION_RATIO);
        distanceMultiplier = environment.getProperty(DISTANCE_MULTIPLIER_PROPERTY, Float.class,
                IndexBuilder.DEFAULT_DISTANCE_MULTIPLIER);

        basePath = Path.of(environment.getProperty(BASE_PATH_PROPERTY, String.class, "."));

        var availableRAM = fetchAvailableRAM();
        if (availableRAM >= EIGHT_TB) {
            var msg = "Unable to detect amount of RAM available on server";
            logger.error(msg);
            throw new IllegalArgumentException(msg);
        }

        var heapSize = Runtime.getRuntime().maxMemory();
        var leftMemory = availableRAM - heapSize;
        var osMemory = Math.min(leftMemory / 10, 1024L * 1024 * 1024);

        long maxMemoryConsumption = leftMemory - osMemory;
        logger.info("Direct memory size : " + maxMemoryConsumption + " bytes, " +
                "heap size : " + heapSize + " bytes, available RAM " + availableRAM +
                " bytes, memory left for OS needs " + osMemory + " bytes");

        if (!environment.containsProperty(INDEX_BUILDING_MAX_MEMORY_CONSUMPTION_PROPERTY)) {
            indexBuildingMaxMemoryConsumption = maxMemoryConsumption / 2;

            logger.info("Property " + INDEX_BUILDING_MAX_MEMORY_CONSUMPTION_PROPERTY + " is not set. " +
                    "Using " + indexBuildingMaxMemoryConsumption + " bytes for index building. "
                    + indexBuildingMaxMemoryConsumption + " bytes will be used for disk page cache.");
        } else {
            var memoryConsumption = environment.getProperty(INDEX_BUILDING_MAX_MEMORY_CONSUMPTION_PROPERTY, Long.class);

            if (memoryConsumption == null) {
                var msg = "Property " + INDEX_BUILDING_MAX_MEMORY_CONSUMPTION_PROPERTY +
                        " is not a valid long value.";
                logger.error(msg);
                throw new IllegalArgumentException(msg);
            }

            indexBuildingMaxMemoryConsumption = memoryConsumption;
            logger.info("Using " + indexBuildingMaxMemoryConsumption + " bytes for index building. " +
                    (maxMemoryConsumption - indexBuildingMaxMemoryConsumption) +
                    " bytes will be used for disk page cache.");
        }

        if (!environment.containsProperty(INDEX_READER_DISK_CACHE_MEMORY_CONSUMPTION)) {
            diskCacheMemoryConsumption = maxMemoryConsumption / 2;

            logger.info("Property " + INDEX_READER_DISK_CACHE_MEMORY_CONSUMPTION + " is not set. " +
                    "Using " + diskCacheMemoryConsumption + " bytes for disk page cache." + diskCacheMemoryConsumption +
                    " bytes will be used to keep primary index in memory.");
        } else {
            var memoryConsumption = environment.getProperty(INDEX_READER_DISK_CACHE_MEMORY_CONSUMPTION, Long.class);

            if (memoryConsumption == null) {
                var msg = "Property " + INDEX_READER_DISK_CACHE_MEMORY_CONSUMPTION +
                        " is not a valid long value.";
                logger.error(msg);
                throw new IllegalArgumentException(msg);
            }

            diskCacheMemoryConsumption = memoryConsumption;
            logger.info("Using " + diskCacheMemoryConsumption + " bytes for disk page cache." +
                    (maxMemoryConsumption - diskCacheMemoryConsumption) +
                    " bytes will be used to keep primary index in memory.");
        }

        var modeName = environment.getProperty(DEFAULT_MODE_PROPERTY, String.class, BUILD_MODE).toLowerCase(Locale.ROOT);

        if (modeName.equals(BUILD_MODE)) {
            mode = new BuildMode();
        } else if (modeName.equals(SEARCH_MODE)) {
            mode = new SearchMode();
        } else {
            var msg = "Unknown mode " + modeName;
            logger.error(msg);
            throw new IllegalArgumentException(msg);
        }


        logger.info("Index manager initialized with parameters " +
                        "dimensions = {}, " +
                        "maxConnectionsPerVertex = {}, " +
                        "maxCandidatesReturned = {}, " +
                        "compressionRatio = {}, " +
                        "distanceMultiplier = {}, " +
                        "mode = {}",
                dimensions, maxConnectionsPerVertex, maxCandidatesReturned, compressionRatio,
                distanceMultiplier, modeName);
    }


    @Override
    public void createIndex(IndexManagerOuterClass.CreateIndexRequest request,
                            StreamObserver<IndexManagerOuterClass.CreateIndexResponse> responseObserver) {
        operationsSemaphore.acquireUninterruptibly();
        try {
            if (closed) {
                responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
                return;
            }

            mode.createIndex(request, responseObserver);
        } finally {
            operationsSemaphore.release();
        }
    }


    @Override
    public void buildIndex(IndexManagerOuterClass.IndexNameRequest request, StreamObserver<Empty> responseObserver) {
        operationsSemaphore.acquireUninterruptibly();
        try {
            if (closed) {
                responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
                return;
            }

            mode.buildIndex(request, responseObserver);
        } finally {
            operationsSemaphore.release();
        }
    }

    @Override
    public StreamObserver<IndexManagerOuterClass.UploadVectorsRequest> uploadVectors(
            StreamObserver<Empty> responseObserver) {

        operationsSemaphore.acquireUninterruptibly();
        if (closed) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
            operationsSemaphore.release();
            return null;
        }

        return mode.uploadVectors(responseObserver);

    }

    @Override
    public void buildStatus(final Empty request,
                            final StreamObserver<IndexManagerOuterClass.BuildStatusResponse> responseObserver) {
        operationsSemaphore.acquireUninterruptibly();
        try {
            if (closed) {
                responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
                return;
            }

            mode.buildStatus(request, responseObserver);
        } finally {
            operationsSemaphore.release();
        }
    }

    @Override
    public void indexState(IndexManagerOuterClass.IndexNameRequest request,
                           StreamObserver<IndexManagerOuterClass.IndexStateResponse> responseObserver) {
        operationsSemaphore.acquireUninterruptibly();
        try {
            if (closed) {
                responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
                return;
            }

            try {
                var indexName = request.getIndexName();
                var state = indexStates.get(indexName);
                if (state == null) {
                    responseObserver.onError(new StatusException(Status.NOT_FOUND));
                } else {
                    responseObserver.onNext(IndexManagerOuterClass.IndexStateResponse.newBuilder()
                            .setState(convertToAPIState(state))
                            .build());
                    responseObserver.onCompleted();
                }
            } catch (StatusRuntimeException e) {
                responseObserver.onError(e);
            } catch (Exception e) {
                responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
            }
        } finally {
            operationsSemaphore.release();
        }
    }

    @Override
    public void list(Empty request, StreamObserver<IndexManagerOuterClass.IndexListResponse> responseObserver) {
        operationsSemaphore.acquireUninterruptibly();
        try {
            if (closed) {
                responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
                return;
            }

            var responseBuilder = IndexManagerOuterClass.IndexListResponse.newBuilder();
            try {
                for (var entry : indexStates.entrySet()) {
                    if (entry.getValue() != IndexState.BROKEN) {
                        responseBuilder.addIndexNames(entry.getKey());
                    }
                }

                responseObserver.onNext(responseBuilder.build());
                responseObserver.onCompleted();
            } catch (StatusRuntimeException e) {
                responseObserver.onError(e);
            } catch (Exception e) {
                logger.error("Failed to list indexes", e);
                responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
            }
        } finally {
            operationsSemaphore.release();
        }

    }

    @Override
    public void switchToBuildMode(Empty request, StreamObserver<Empty> responseObserver) {
        var releasePermits = false;
        modeLock.lock();
        try {
            if (mode instanceof BuildMode) {
                logger.info("Will not switch to build mode, because it is already active");

                responseObserver.onNext(Empty.newBuilder().build());
                responseObserver.onCompleted();
                return;
            }

            if (!operationsSemaphore.tryAcquire(Integer.MAX_VALUE)) {
                logger.error("Failed to switch to build mode because of ongoing operations");
                responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
                return;
            }

            if (closed) {
                responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
                return;
            }

            releasePermits = true;

            mode.shutdown();

            mode = new BuildMode();
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();

        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
        } catch (Exception e) {
            logger.error("Failed to switch to build mode", e);
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
        } finally {
            if (releasePermits) {
                operationsSemaphore.release(Integer.MAX_VALUE);
            }
            modeLock.unlock();
        }
    }

    @Override
    public void switchToSearchMode(Empty request, StreamObserver<Empty> responseObserver) {
        modeLock.lock();
        var releasePermits = false;

        try {
            if (mode instanceof SearchMode) {
                logger.info("Will not switch to search mode, because it is already active");

                responseObserver.onNext(Empty.newBuilder().build());
                responseObserver.onCompleted();
                return;
            }

            if (!operationsSemaphore.tryAcquire(Integer.MAX_VALUE)) {
                var msg = "Failed to switch to search mode because of ongoing operations";
                logger.error(msg);

                responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE.withDescription(msg)));
                return;
            }

            if (closed) {
                responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
                return;
            }

            releasePermits = true;
            mode.shutdown();

            mode = new SearchMode();
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
        } catch (Exception e) {
            logger.error("Failed to switch to search mode", e);
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
        } finally {
            if (releasePermits) {
                operationsSemaphore.release(Integer.MAX_VALUE);
            }
            modeLock.unlock();
        }
    }

    @Override
    public void findNearestNeighbours(IndexManagerOuterClass.FindNearestNeighboursRequest request,
                                      StreamObserver<IndexManagerOuterClass.FindNearestNeighboursResponse> responseObserver) {
        operationsSemaphore.acquireUninterruptibly();
        try {
            if (closed) {
                responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
                return;
            }

            mode.findNearestNeighbours(request, responseObserver);
        } finally {
            operationsSemaphore.release();
        }
    }

    @Override
    public void dropIndex(IndexManagerOuterClass.IndexNameRequest request, StreamObserver<Empty> responseObserver) {
        operationsSemaphore.acquireUninterruptibly();
        try {
            if (closed) {
                responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE));
                return;
            }

            mode.dropIndex(request, responseObserver);
        } finally {
            operationsSemaphore.release();
        }
    }

    private IndexManagerOuterClass.IndexState convertToAPIState(IndexState indexState) {
        return switch (indexState) {
            case BROKEN -> IndexManagerOuterClass.IndexState.BROKEN;
            case BUILDING -> IndexManagerOuterClass.IndexState.BUILDING;
            case BUILT -> IndexManagerOuterClass.IndexState.BUILT;
            case CREATED -> IndexManagerOuterClass.IndexState.CREATED;
            case CREATING -> IndexManagerOuterClass.IndexState.CREATING;
            case IN_BUILD_QUEUE -> IndexManagerOuterClass.IndexState.IN_BUILD_QUEUE;
            case UPLOADING -> IndexManagerOuterClass.IndexState.UPLOADING;
        };
    }

    private static long fetchAvailableRAM() {
        var osName = System.getProperty("os.name").toLowerCase(Locale.US);
        if (osName.contains("linux")) {
            return availableMemoryLinux();
        } else if (osName.contains("windows")) {
            return availableMemoryWindows();
        }

        return Integer.MAX_VALUE;
    }

    private static long availableMemoryWindows() {
        try (var arena = Arena.openShared()) {
            var memoryStatusExLayout = MemoryLayout.structLayout(
                    ValueLayout.JAVA_INT.withName("dwLength"),
                    ValueLayout.JAVA_INT.withName("dwMemoryLoad"),
                    ValueLayout.JAVA_LONG.withName("ullTotalPhys"),
                    ValueLayout.JAVA_LONG.withName("ullAvailPhys"),
                    ValueLayout.JAVA_LONG.withName("ullTotalPageFile"),
                    ValueLayout.JAVA_LONG.withName("ullAvailPageFile"),
                    ValueLayout.JAVA_LONG.withName("ullTotalVirtual"),
                    ValueLayout.JAVA_LONG.withName("ullAvailVirtual"),
                    ValueLayout.JAVA_LONG.withName("ullAvailExtendedVirtual")
            );

            var memoryStatusExSize = memoryStatusExLayout.byteSize();
            var memoryStatusExSegment = arena.allocate(memoryStatusExLayout);

            memoryStatusExSegment.set(ValueLayout.JAVA_LONG, memoryStatusExLayout.byteOffset(
                    MemoryLayout.PathElement.groupElement("dwLength")), memoryStatusExSize);

            var linker = Linker.nativeLinker();

            var lookup = SymbolLookup.libraryLookup("kernel32.dll", arena.scope());
            var globalMemoryStatusExOptional = lookup.find("GlobalMemoryStatusEx");
            if (globalMemoryStatusExOptional.isEmpty()) {
                logger.error("Failed to find GlobalMemoryStatusEx in kernel32.dll");
                return Integer.MAX_VALUE;
            }

            var globalMemoryStatusEx = globalMemoryStatusExOptional.get();
            var globalMemoryStatusExHandle = linker.downcallHandle(globalMemoryStatusEx,
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.ADDRESS));
            try {
                globalMemoryStatusExHandle.invoke(memoryStatusExSegment);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }


            return memoryStatusExSegment.get(ValueLayout.JAVA_LONG,
                    memoryStatusExLayout.byteOffset(MemoryLayout.PathElement.groupElement("ullTotalPhys")));
        }
    }

    private static long availableMemoryLinux() {
        var memInfoMemory = fetchMemInfoMemory();
        var cGroupV1Memory = fetchCGroupV1Memory();
        var cGroupV2Memory = fetchCGroupV2Memory();

        return Math.min(memInfoMemory, Math.min(cGroupV1Memory, cGroupV2Memory));
    }

    private static long fetchMemInfoMemory() {
        try (var bufferedReader = new BufferedReader(new FileReader("/proc/meminfo"))) {

            String memTotalLine = bufferedReader.readLine();

            String[] memTotalParts = memTotalLine.split("\\s+");
            return Long.parseLong(memTotalParts[1]) * 1024;
        } catch (IOException e) {
            logger.error("Failed to read /proc/meminfo", e);
            return Integer.MAX_VALUE;
        }
    }

    private static long fetchCGroupV1Memory() {
        if (!Files.exists(Path.of("/sys/fs/cgroup/memory/memory.limit_in_bytes"))) {
            return Long.MAX_VALUE;
        }

        try (var bufferedReader = new BufferedReader(new FileReader("/sys/fs/cgroup/memory/memory.limit_in_bytes"))) {
            String memoryLimitLine = bufferedReader.readLine();
            return Long.parseLong(memoryLimitLine.split("\\s+")[0]);
        } catch (IOException | NumberFormatException e) {
            logger.error("Failed to read /sys/fs/cgroup/memory/memory.limit_in_bytes", e);
            return Integer.MAX_VALUE;
        }
    }

    private static long fetchCGroupV2Memory() {
        if (!Files.exists(Path.of("/sys/fs/cgroup/memory.max"))) {
            return Long.MAX_VALUE;
        }

        try (var bufferedReader = new BufferedReader(new FileReader("/sys/fs/cgroup/memory.max"))) {
            String memoryLimitLine = bufferedReader.readLine();
            return Long.parseLong(memoryLimitLine.split("\\s+")[0]);
        } catch (IOException | NumberFormatException e) {
            logger.error("Failed to read /sys/fs/cgroup/memory.max", e);
            return Integer.MAX_VALUE;
        }
    }


    @PreDestroy
    public void shutdown() {
        operationsSemaphore.acquireUninterruptibly(Integer.MAX_VALUE);
        try {
            closed = true;

            mode.shutdown();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            operationsSemaphore.release();
        }
    }

    private final class IndexBuilderTask implements Runnable {
        private final String indexName;

        public IndexBuilderTask(String indexName) {
            this.indexName = indexName;
        }

        @Override
        public void run() {
            operationsSemaphore.acquireUninterruptibly();
            try {
                try {
                    var metadata = indexMetadatas.get(indexName);
                    if (indexStates.replace(indexName, IndexState.IN_BUILD_QUEUE, IndexState.BUILDING)) {
                        try {
                            IndexBuilder.buildIndex(indexName, dimensions, compressionRatio,
                                    distanceMultiplier, metadata.dir,
                                    DataStore.dataLocation(indexName, metadata.dir),
                                    indexBuildingMaxMemoryConsumption, maxConnectionsPerVertex,
                                    maxCandidatesReturned,
                                    metadata.distance, progressTracker);
                        } catch (Exception e) {
                            logger.error("Failed to build index " + indexName, e);
                            indexStates.put(indexName, IndexState.BROKEN);
                            return;
                        }

                        indexStates.put(indexName, IndexState.BUILT);
                    } else {
                        logger.warn("Failed to build index " + indexName + " because it is not in IN_BUILD_QUEUE state");
                    }
                } catch (Throwable t) {
                    logger.error("Index builder task failed", t);
                    throw t;
                }
            } finally {
                operationsSemaphore.release();
            }

        }
    }

    private record IndexMetadata(Distance distance, Path dir) {
    }

    private interface Mode {
        void createIndex(IndexManagerOuterClass.CreateIndexRequest request,
                         StreamObserver<IndexManagerOuterClass.CreateIndexResponse> responseObserver);

        void buildIndex(IndexManagerOuterClass.IndexNameRequest request, StreamObserver<Empty> responseObserver);

        StreamObserver<IndexManagerOuterClass.UploadVectorsRequest> uploadVectors(
                StreamObserver<Empty> responseObserver);

        void buildStatus(final Empty request,
                         final StreamObserver<IndexManagerOuterClass.BuildStatusResponse> responseObserver);

        void findNearestNeighbours(IndexManagerOuterClass.FindNearestNeighboursRequest request,
                                   StreamObserver<IndexManagerOuterClass.FindNearestNeighboursResponse> responseObserver);

        void dropIndex(IndexManagerOuterClass.IndexNameRequest request, StreamObserver<Empty> responseObserver);

        void shutdown() throws IOException;
    }

    private final class SearchMode implements Mode {
        private final DiskCache diskCache;

        private final ConcurrentHashMap<String, IndexReader> indexReaders = new ConcurrentHashMap<>();

        private SearchMode() {
            diskCache = new DiskCache(diskCacheMemoryConsumption, dimensions, maxConnectionsPerVertex);
        }

        @Override
        public void createIndex(IndexManagerOuterClass.CreateIndexRequest request,
                                StreamObserver<IndexManagerOuterClass.CreateIndexResponse> responseObserver) {
            searchOnly(responseObserver);
        }

        @Override
        public void buildIndex(IndexManagerOuterClass.IndexNameRequest request, StreamObserver<Empty> responseObserver) {
            searchOnly(responseObserver);
        }

        @Override
        public StreamObserver<IndexManagerOuterClass.UploadVectorsRequest> uploadVectors(StreamObserver<Empty> responseObserver) {
            searchOnly(responseObserver);
            return null;
        }

        @Override
        public void buildStatus(Empty request, StreamObserver<IndexManagerOuterClass.BuildStatusResponse> responseObserver) {
            searchOnly(responseObserver);
        }

        @Override
        public void findNearestNeighbours(IndexManagerOuterClass.FindNearestNeighboursRequest request,
                                          StreamObserver<IndexManagerOuterClass.FindNearestNeighboursResponse> responseObserver) {
            var indexName = request.getIndexName();
            if (checkBuildState(responseObserver, indexName)) {
                return;
            }

            var responseBuilder = IndexManagerOuterClass.FindNearestNeighboursResponse.newBuilder();
            try {
                @SuppressWarnings("resource") var indexReader = fetchIndexReader(indexName);

                var neighboursCount = request.getK();
                var queryVector = request.getVectorComponentsList();
                var result = new int[neighboursCount];

                var vector = new float[dimensions];
                for (int i = 0; i < dimensions; i++) {
                    vector[i] = queryVector.get(i);
                }

                indexReader.nearest(vector, result, neighboursCount);


                for (var vectorIndex : result) {
                    responseBuilder.addIds(vectorIndex);
                }
            } catch (Exception e) {
                logger.error("Failed to find nearest neighbours", e);
                responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));

                return;
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void dropIndex(IndexManagerOuterClass.IndexNameRequest request, StreamObserver<Empty> responseObserver) {
            var indexName = request.getIndexName();
            if (checkBuildState(responseObserver, indexName)) {
                return;
            }

            try {
                @SuppressWarnings("resource") var indexReader = fetchIndexReader(indexName);
                indexReader.deleteIndex();

                //noinspection resource
                indexReaders.remove(indexName);
                indexStates.remove(indexName);
                indexMetadatas.remove(indexName);

                responseObserver.onNext(Empty.newBuilder().build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                logger.error("Failed dropping an index '" + indexName + "'", e);
                responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
            }
        }

        @NotNull
        private IndexReader fetchIndexReader(final String indexName) {
            return indexReaders.computeIfAbsent(indexName, r -> {
                var metadata = indexMetadatas.get(indexName);
                return new IndexReader(indexName, dimensions, maxConnectionsPerVertex, maxCandidatesReturned,
                        compressionRatio, metadata.dir, metadata.distance, diskCache);

            });
        }

        private boolean checkBuildState(final StreamObserver<?> responseObserver, final String indexName) {
            var indexState = indexStates.get(indexName);

            if (indexState != IndexState.BUILT) {
                var msg = "Index " + indexName + " is not in BUILT state";
                logger.error(msg);

                responseObserver.onError(new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription(msg)));
                return true;
            }

            return false;
        }

        @Override
        public void shutdown() throws IOException {
            for (var indexReader : indexReaders.values()) {
                indexReader.close();
            }

            diskCache.close();
        }

        private void searchOnly(StreamObserver<?> responseObserver) {
            responseObserver.onError(new StatusRuntimeException(
                    Status.PERMISSION_DENIED.withDescription("Index manager is in search mode")));
        }
    }

    private final class BuildMode implements Mode {
        private final ExecutorService indexBuilderExecutor;
        private final ReentrantLock indexCreationLock = new ReentrantLock();


        private BuildMode() {

            indexBuilderExecutor = Executors.newFixedThreadPool(1, r -> {
                var thread = new Thread(r, "Index builder");
                thread.setDaemon(true);
                return thread;
            });
        }

        @Override
        public void createIndex(IndexManagerOuterClass.CreateIndexRequest request,
                                StreamObserver<IndexManagerOuterClass.CreateIndexResponse> responseObserver) {
            indexCreationLock.lock();
            try {
                var indexName = request.getIndexName();
                var indexState = indexStates.putIfAbsent(indexName, IndexState.CREATING);

                if (indexState != null) {
                    var msg = "Index " + indexName + " already exists";
                    logger.error(msg);

                    responseObserver.onError(new StatusRuntimeException(Status.ALREADY_EXISTS.withDescription(msg)));
                    return;
                }

                var responseBuilder = IndexManagerOuterClass.CreateIndexResponse.newBuilder();
                try {
                    var indexDir = basePath.resolve(indexName);
                    Files.createDirectories(indexDir);

                    var statusFilePath = indexDir.resolve("status");
                    Files.writeString(statusFilePath, IndexState.CREATED.name(), StandardOpenOption.SYNC,
                            StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

                    indexMetadatas.put(indexName,
                            new IndexMetadata(Distance.valueOf(request.getDistance().name()), indexDir));
                    if (!indexStates.replace(indexName, IndexState.CREATING, IndexState.CREATED)) {
                        var msg = "Failed to create index " + indexName;
                        logger.error(msg);
                        responseObserver.onError(new IllegalStateException(msg));

                        indexStates.put(indexName, IndexState.BROKEN);
                        Files.writeString(statusFilePath, IndexState.BROKEN.name(), StandardOpenOption.WRITE,
                                StandardOpenOption.SYNC);
                    }

                    responseObserver.onNext(responseBuilder.build());
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    indexMetadatas.remove(indexName);
                    logger.error("Failed to create index " + indexName, e);
                    responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
                }
            } catch (StatusRuntimeException e) {
                responseObserver.onError(e);
            } catch (Exception e) {
                logger.error("Failed to create index", e);
                responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
            } finally {
                indexCreationLock.unlock();
            }
        }

        @Override
        public void buildIndex(IndexManagerOuterClass.IndexNameRequest request, StreamObserver<Empty> responseObserver) {
            try {
                var indexName = request.getIndexName();
                var indexState = indexStates.compute(indexName, (k, state) -> {
                    if (state == IndexState.CREATED) {
                        return IndexState.IN_BUILD_QUEUE;
                    } else {
                        return state;
                    }
                });

                if (indexState == null) {
                    var msg = "Index " + indexName + " does not exist";
                    logger.error(msg);

                    responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription(msg)));
                    return;
                }

                if (indexState != IndexState.IN_BUILD_QUEUE) {
                    var msg = "Index " + indexName + " is not in CREATED state";
                    logger.error(msg);

                    responseObserver.onError(new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription(msg)));
                    return;
                }

                indexBuilderExecutor.execute(new IndexBuilderTask(indexName));

                responseObserver.onNext(Empty.newBuilder().build());
                responseObserver.onCompleted();
            } catch (StatusRuntimeException e) {
                responseObserver.onError(e);
            } catch (Exception e) {
                responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
            }
        }

        @Override
        public StreamObserver<IndexManagerOuterClass.UploadVectorsRequest> uploadVectors(StreamObserver<Empty> responseObserver) {
            return new StreamObserver<>() {
                private DataStore store;
                private String indexName;

                @Override
                public void onNext(IndexManagerOuterClass.UploadVectorsRequest value) {
                    var indexName = value.getIndexName();
                    if (this.indexName == null) {
                        if (!indexStates.replace(indexName, IndexState.CREATED, IndexState.UPLOADING)) {
                            var msg = "Index " + indexName + " is not in CREATED state";
                            logger.error(msg);

                            responseObserver.onError(
                                    new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription(msg)));
                            return;
                        }

                        uploaderLock.lock();
                        try {
                            if (!uploadingIndexes.contains(indexName)) {
                                if (uploadingIndexes.size() == MAXIMUM_UPLOADERS_COUNT) {
                                    indexStates.put(indexName, IndexState.CREATED);

                                    responseObserver.onError(new StatusRuntimeException(
                                            Status.RESOURCE_EXHAUSTED.withDescription("Maximum uploaders count reached")));
                                    return;
                                }

                                uploadingIndexes.add(indexName);
                            }
                        } finally {
                            uploaderLock.unlock();
                        }

                        var metadata = indexMetadatas.get(indexName);

                        try {
                            store = DataStore.create(indexName, dimensions, metadata.distance.buildDistanceFunction(),
                                    metadata.dir);
                        } catch (IOException e) {
                            var msg = "Failed to create data store for index " + indexName;
                            logger.error(msg, e);

                            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));

                            indexStates.put(indexName, IndexState.BROKEN);
                        }

                        this.indexName = indexName;
                    } else {
                        var indexState = indexStates.get(indexName);
                        if (indexState != IndexState.UPLOADING) {
                            var msg = "Index " + indexName + " is not in UPLOADING state";
                            logger.error(msg);

                            responseObserver.onError(
                                    new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription(msg)));
                            return;
                        }

                        if (!indexName.equals(this.indexName)) {
                            var msg = "Index name mismatch: expected " + this.indexName + ", got " + indexName;
                            logger.error(msg);
                            responseObserver.onError(
                                    new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription(msg)));
                        }
                    }

                    var componentsCount = value.getVectorComponentsCount();
                    var indexMetadata = IndexManagerServiceImpl.this.indexMetadatas.get(indexName);
                    if (indexMetadata == null) {
                        var msg = "Index " + indexName + " does not exist";
                        logger.error(msg);

                        responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND.withDescription(msg)));
                        return;
                    }

                    if (componentsCount != dimensions) {
                        var msg = "Index " + indexName + " has " + dimensions + " dimensions, but " +
                                componentsCount + " were provided";
                        logger.error(msg);

                        responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(msg)));
                        return;
                    }


                    var vector = new float[componentsCount];
                    for (var i = 0; i < componentsCount; i++) {
                        vector[i] = value.getVectorComponents(i);
                    }
                    try {
                        store.add(vector);
                    } catch (IOException e) {
                        var msg = "Failed to add vector to index " + indexName;
                        logger.error(msg, e);

                        responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
                        indexStates.put(indexName, IndexState.BROKEN);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    try {
                        var indexName = this.indexName;

                        if (indexName != null) {
                            indexStates.put(indexName, IndexState.BROKEN);
                            uploadingIndexes.remove(indexName);
                        }
                        logger.error("Failed to upload vectors for index " + indexName, t);
                        try {
                            if (store != null) {
                                store.close();
                            }
                        } catch (IOException e) {
                            logger.error("Failed to close data store for index " + indexName, e);
                        }

                        responseObserver.onError(t);
                    } finally {
                        operationsSemaphore.release();
                    }
                }

                @Override
                public void onCompleted() {
                    try {
                        try {
                            if (store != null) {
                                store.close();
                            }

                            uploadingIndexes.remove(indexName);
                            indexStates.put(indexName, IndexState.CREATED);
                        } catch (IOException e) {
                            var msg = "Failed to close data store for index " + indexName;
                            logger.error(msg, e);
                            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
                        }

                        responseObserver.onCompleted();
                    } finally {
                        operationsSemaphore.release();
                    }
                }
            };
        }

        @Override
        public void buildStatus(Empty request,
                                StreamObserver<IndexManagerOuterClass.BuildStatusResponse> responseObserver) {
            var buildListener = new ServiceIndexBuildProgressListener(responseObserver);
            progressTracker.addListener(buildListener);
        }

        @Override
        public void findNearestNeighbours(IndexManagerOuterClass.FindNearestNeighboursRequest request,
                                          StreamObserver<IndexManagerOuterClass.FindNearestNeighboursResponse> responseObserver) {
            responseObserver.onError(new StatusRuntimeException(Status.UNAVAILABLE.augmentDescription(
                    "Index manager is in build mode. Please switch to search mode.")));
        }

        @Override
        public void dropIndex(IndexManagerOuterClass.IndexNameRequest request, StreamObserver<Empty> responseObserver) {
            indexCreationLock.lock();
            try {
                var indexName = request.getIndexName();

                indexStates.compute(indexName, (k, state) -> {
                    if (state == IndexState.CREATED || state == IndexState.BUILT) {
                        return IndexState.BROKEN;
                    } else {
                        return state;
                    }
                });

                var state = indexStates.get(indexName);
                if (state != IndexState.BROKEN) {
                    var msg = "Index " + request.getIndexName() + " is not in CREATED or BUILT state";
                    logger.error(msg);

                    responseObserver.onError(new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription(msg)));
                    return;
                }

                var indexDir = indexMetadatas.get(indexName).dir;
                FileUtils.deleteDirectory(indexDir.toFile());

                indexMetadatas.remove(indexName);
                indexStates.remove(indexName);
                responseObserver.onCompleted();
            } catch (Exception e) {
                indexStates.put(request.getIndexName(), IndexState.BROKEN);
                logger.error("Failed to drop index " + request.getIndexName(), e);
                responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
            } finally {
                indexCreationLock.unlock();
            }
        }

        @Override
        public void shutdown() {
            for (var indexes : indexStates.keySet()) {
                var indexState = indexStates.get(indexes);

                if (indexState == IndexState.IN_BUILD_QUEUE || indexState == IndexState.BUILDING) {
                    indexStates.put(indexes, IndexState.BROKEN);
                }
            }

            indexBuilderExecutor.shutdown();
        }
    }

    private class ServiceIndexBuildProgressListener implements IndexBuildProgressListener {
        private final Context context;
        private final StreamObserver<IndexManagerOuterClass.BuildStatusResponse> responseObserver;

        public ServiceIndexBuildProgressListener(StreamObserver<IndexManagerOuterClass.BuildStatusResponse> responseObserver) {
            this.responseObserver = responseObserver;
            context = Context.current();
        }

        @Override
        public void progress(IndexBuildProgressInfo progressInfo) {
            if (context.isCancelled()) {
                try {
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    progressTracker.removeListener(this);
                    responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
                }
            }

            try {
                var responseBuilder = IndexManagerOuterClass.BuildStatusResponse.newBuilder();
                responseBuilder.setIndexName(progressInfo.indexName());

                for (@SuppressWarnings("ForEachWithRecordPatternCanBeUsed") var phase : progressInfo.phases()) {
                    var name = phase.name();
                    var progress = phase.progress();
                    var parameters = phase.parameters();

                    var phaseBuilder = IndexManagerOuterClass.BuildPhase.newBuilder();

                    phaseBuilder.setName(name);
                    phaseBuilder.setCompletionPercentage(progress);
                    for (var parameter : parameters) {
                        phaseBuilder.addParameters(parameter);
                    }

                    responseBuilder.addPhases(phaseBuilder);
                }


                responseObserver.onNext(responseBuilder.build());
            } catch (StatusRuntimeException e) {
                if (e.getStatus() == Status.CANCELLED) {
                    progressTracker.removeListener(this);
                    responseObserver.onCompleted();
                } else {
                    logger.error("Failed to send build status", e);
                    progressTracker.removeListener(this);
                    responseObserver.onError(e);
                }
            } catch (Exception e) {
                logger.error("Failed to send build status", e);
                progressTracker.removeListener(this);
                responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
            }
        }
    }
}
