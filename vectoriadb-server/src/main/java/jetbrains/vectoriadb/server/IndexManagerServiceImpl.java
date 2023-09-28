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
import jetbrains.vectoriadb.service.base.IndexManagerGrpc;
import jetbrains.vectoriadb.service.base.IndexManagerOuterClass;
import net.devh.boot.grpc.server.service.GrpcService;
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
import java.util.concurrent.locks.ReentrantLock;

@GrpcService
public class IndexManagerServiceImpl extends IndexManagerGrpc.IndexManagerImplBase {
    private static final long EIGHT_TB = 8L * 1024 * 1024 * 1024 * 1024;

    public static final String DIMENSIONS_PROPERTY = "vector-index.dimensions";
    public static final String MAX_CONNECTIONS_PER_VERTEX_PROPERTY = "vector-index.max-connections-per-vertex";
    public static final String MAX_CANDIDATES_RETURNED_PROPERTY = "vector-index.max-candidates-returned";
    public static final String COMPRESSION_RATIO_PROPERTY = "vector-index.compression-ratio";
    public static final String DISTANCE_MULTIPLIER_PROPERTY = "vector-index.distance-multiplier";
    public static final String INDEX_BUILDING_MAX_MEMORY_CONSUMPTION_PROPERTY = "vector-index.building.max-memory-consumption";
    private static final int DEFAULT_DIMENSIONS = 128;
    private static final Logger logger = LoggerFactory.getLogger(IndexManagerServiceImpl.class);
    private final ConcurrentHashMap<String, IndexState> indexStates = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, IndexMetadata> indexMetadatas = new ConcurrentHashMap<>();

    private final ListenerBasedPeriodicProgressTracker progressTracker = new ListenerBasedPeriodicProgressTracker(5);

    private static final int MAXIMUM_UPLOADERS_COUNT = 64;

    private final Set<String> uploadingIndexes = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final ReentrantLock uploaderLock = new ReentrantLock();

    private final ExecutorService indexBuilderExecutor;
    private final int dimensions;
    private final int maxConnectionsPerVertex;
    private final int maxCandidatesReturned;
    private final int compressionRatio;
    private final float distanceMultiplier;
    private final long indexBuildingMaxMemoryConsumption;

    public IndexManagerServiceImpl(Environment environment) {
        dimensions = environment.getProperty(DIMENSIONS_PROPERTY, Integer.class, DEFAULT_DIMENSIONS);

        maxConnectionsPerVertex = environment.getProperty(MAX_CONNECTIONS_PER_VERTEX_PROPERTY, Integer.class,
                IndexBuilder.DEFAULT_MAX_CONNECTIONS_PER_VERTEX);
        maxCandidatesReturned = environment.getProperty(MAX_CANDIDATES_RETURNED_PROPERTY, Integer.class,
                IndexBuilder.DEFAULT_MAX_AMOUNT_OF_CANDIDATES);
        compressionRatio = environment.getProperty(COMPRESSION_RATIO_PROPERTY, Integer.class,
                IndexBuilder.DEFAULT_COMPRESSION_RATIO);
        distanceMultiplier = environment.getProperty(DISTANCE_MULTIPLIER_PROPERTY, Float.class,
                IndexBuilder.DEFAULT_DISTANCE_MULTIPLIER);

        if (!environment.containsProperty(INDEX_BUILDING_MAX_MEMORY_CONSUMPTION_PROPERTY)) {
            var availableRAM = fetchAvailableRAM();

            if (availableRAM >= EIGHT_TB) {
                var msg = "Property " + INDEX_BUILDING_MAX_MEMORY_CONSUMPTION_PROPERTY +
                        " is not set.";
                logger.error(msg);
                throw new IllegalArgumentException(msg);
            }

            var heapSize = Runtime.getRuntime().maxMemory();
            var leftMemory = availableRAM - heapSize;
            var osMemory = Math.min(leftMemory / 10, 1024L * 1024 * 1024);

            indexBuildingMaxMemoryConsumption = (leftMemory - osMemory) / 2;

            logger.info("Property " + INDEX_BUILDING_MAX_MEMORY_CONSUMPTION_PROPERTY + " is not set. " +
                    "Using " + indexBuildingMaxMemoryConsumption + " bytes for index building. " +
                    "Heap size : " + heapSize + " bytes, available RAM " + availableRAM +
                    " bytes, memory left for OS needs " + osMemory + " bytes. The rest of "
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
        }

        indexBuilderExecutor = Executors.newFixedThreadPool(1, r -> {
            var thread = new Thread(r, "Index builder");
            thread.setDaemon(true);
            return thread;
        });

        logger.info("Index manager initialized with parameters " +
                        "dimensions = {}, " +
                        "maxConnectionsPerVertex = {}, " +
                        "maxCandidatesReturned = {}, " +
                        "compressionRatio = {}, " +
                        "distanceMultiplier = {}, " +
                        "indexBuildingMaxMemoryConsumption = {}",
                dimensions, maxConnectionsPerVertex, maxCandidatesReturned, compressionRatio, distanceMultiplier,
                indexBuildingMaxMemoryConsumption);
    }

    @Override
    public void createIndex(IndexManagerOuterClass.CreateIndexRequest request,
                            StreamObserver<IndexManagerOuterClass.CreateIndexResponse> responseObserver) {
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
                var indexDir = Path.of(indexName);
                Files.createDirectory(indexDir);

                var statusFilePath = indexDir.resolve("status");
                Files.writeString(statusFilePath, IndexState.CREATED.name(), StandardOpenOption.SYNC,
                        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

                indexMetadatas.put(indexName, new IndexMetadata(Distance.valueOf(request.getDistance().name())));
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
            } catch (IOException e) {
                logger.error("Failed to create index " + indexName, e);
                responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
            }
        } catch (StatusRuntimeException e) {
            responseObserver.onError(e);
        } catch (Exception e) {
            logger.error("Failed to create index", e);
            responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
        }
    }

    @Override
    public void buildIndex(IndexManagerOuterClass.IndexNameRequest request, StreamObserver<Empty> responseObserver) {
        try {
            var indexName = request.getIndexName();
            var indexState = indexStates.compute(indexName, (name, state) -> {
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
    public StreamObserver<IndexManagerOuterClass.UploadVectorsRequest> uploadVectors(
            StreamObserver<Empty> responseObserver) {
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

                    var indexDir = Path.of(indexName);
                    var metadata = indexMetadatas.get(indexName);

                    try {
                        store = DataStore.create(indexName, dimensions, metadata.distance.buildDistanceFunction(),
                                indexDir);
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
                var indexName = this.indexName;

                if (indexName != null) {
                    indexStates.put(indexName, IndexState.BROKEN);
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
            }

            @Override
            public void onCompleted() {
                try {
                    if (store != null) {
                        store.close();
                    }

                    indexStates.put(indexName, IndexState.CREATED);
                } catch (IOException e) {
                    var msg = "Failed to close data store for index " + indexName;
                    logger.error(msg, e);
                    responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(e)));
                }

                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void buildStatus(final Empty request,
                            final StreamObserver<IndexManagerOuterClass.BuildStatusResponse> responseObserver) {
        var buildListener = new IndexBuildProgressListener() {
            private final Context context = Context.current();

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

                    for (IndexBuildProgressPhase(String name, double progress, String[] parameters) :
                            progressInfo.phases()) {
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
        };

        progressTracker.addListener(buildListener);
    }

    @Override
    public void indexState(IndexManagerOuterClass.IndexNameRequest request,
                           StreamObserver<IndexManagerOuterClass.IndexStateResponse> responseObserver) {
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
    }

    @Override
    public void list(Empty request, StreamObserver<IndexManagerOuterClass.IndexListResponse> responseObserver) {
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
        try (var arena = Arena.openConfined()) {
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


            return memoryStatusExLayout.byteOffset(MemoryLayout.PathElement.groupElement("ullTotalPhys"));
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
        indexBuilderExecutor.shutdown();
    }

    private final class IndexBuilderTask implements Runnable {
        private final String indexName;

        public IndexBuilderTask(String indexName) {
            this.indexName = indexName;
        }

        @Override
        public void run() {
            try {
                assert indexStates.get(indexName) == IndexState.IN_BUILD_QUEUE;

                var indexDir = Path.of(indexName);

                var metadata = indexMetadatas.get(indexName);
                indexStates.put(indexName, IndexState.BUILDING);
                try {
                    IndexBuilder.buildIndex(indexName, dimensions, compressionRatio,
                            distanceMultiplier, indexDir, DataStore.dataLocation(indexName, indexDir),
                            indexBuildingMaxMemoryConsumption, maxConnectionsPerVertex,
                            maxCandidatesReturned,
                            metadata.distance, progressTracker);
                } catch (Exception e) {
                    logger.error("Failed to build index " + indexName, e);
                    indexStates.put(indexName, IndexState.BROKEN);
                } finally {
                    indexStates.put(indexName, IndexState.BUILT);
                }
            } catch (Throwable t) {
                logger.error("Index builder task failed", t);
                throw t;
            }
        }
    }

    private record IndexMetadata(Distance distance) {
    }
}
