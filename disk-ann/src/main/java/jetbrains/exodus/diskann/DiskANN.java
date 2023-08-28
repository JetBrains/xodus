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
package jetbrains.exodus.diskann;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.LongHeapPriorityQueue;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import jetbrains.exodus.diskann.collections.BoundedCircularLongArrayQueue;
import jetbrains.exodus.diskann.collections.BoundedGreedyVertexPriorityQueue;
import jetbrains.exodus.diskann.collections.NonBlockingHashMapLongLong;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.sampling.PermutationSampler;
import org.apache.commons.rng.simple.RandomSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.lang.management.ManagementFactory;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public final class DiskANN implements AutoCloseable {
    private static final int LOGGING_THRESHOLD = 1024 * 1024;

    public static final byte L2_DISTANCE = 0;
    public static final byte DOT_DISTANCE = 1;

    private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

    public static final int PAGE_SIZE_MULTIPLIER = 4 * 1024;

    private static final Logger logger = LoggerFactory.getLogger(DiskANN.class);

    private final int vectorDim;

    private final float distanceMultiplication;

    private final int maxConnectionsPerVertex;

    private final int maxAmountOfCandidates;

    private int pqSubVectorSize;
    private int pqQuantizersCount;

    private int pqCodeBaseSize;

    private int verticesSize = 0;
    private final Arena arena = Arena.openShared();
    private MemorySegment diskCache;

    private volatile boolean closed;


    /**
     * Size of vertex record in bytes.
     * <p>
     * 1. Vector data (4 bytes * vectorDim)
     * 2. Real amount of edges (1 byte)
     * 3. Edges to other vertices (4 bytes * maxConnectionsPerVertex)
     */
    private final int vertexRecordSize;

    /**
     * During calculation of the amount of vertices per page we need to take into account that first byte of
     * each page contains amount of vertices in the index.
     */
    private final int pageSize;

    private final int verticesPerPage;
    private DiskGraph diskGraph;

    private final byte distanceFunction;
    private final long diskRecordVectorsOffset;
    private final long diskRecordEdgesCountOffset;
    private final long diskRecordEdgesOffset;

    private long pqReCalculated = 0;
    private double pqReCalculationError = 0.0;

    //1st dimension quantizer index
    //2nd index of code inside code book
    //3d dimension centroid vector
    private float[][][] pqCentroids;
    private MemorySegment pqVectors;

    private final ThreadLocal<NearestGreedySearchCachedData> nearestGreedySearchCachedDataThreadLocal;

    private final Path path;

    private final String name;


    public DiskANN(String name, final Path path, int vectorDim, byte distanceFunction) {
        this(name, path, vectorDim, distanceFunction, 1.2f,
                64, 128,
                32);
    }

    public DiskANN(String name, Path path, int vectorDim, byte distanceFunction,
                   float distanceMultiplication,
                   int maxConnectionsPerVertex,
                   int maxAmountOfCandidates,
                   int pqCompression) {
        this.name = name;
        this.path = path;
        this.vectorDim = vectorDim;
        this.distanceMultiplication = distanceMultiplication;
        this.maxConnectionsPerVertex = maxConnectionsPerVertex;
        this.maxAmountOfCandidates = maxAmountOfCandidates;
        this.distanceFunction = distanceFunction;

        MemoryLayout diskCacheRecordLayout = MemoryLayout.structLayout(
                MemoryLayout.sequenceLayout(vectorDim, ValueLayout.JAVA_FLOAT).withName("vector"),
                MemoryLayout.sequenceLayout(maxConnectionsPerVertex, ValueLayout.JAVA_INT).withName("edges"),
                ValueLayout.JAVA_BYTE.withName("edgesCount")
        );

        long diskCacheRecordByteAlignment = diskCacheRecordLayout.byteAlignment();
        this.vertexRecordSize = (int) (
                ((diskCacheRecordLayout.byteSize() + diskCacheRecordByteAlignment - 1)
                        / diskCacheRecordByteAlignment) * diskCacheRecordByteAlignment
        );

        diskRecordVectorsOffset = diskCacheRecordLayout.byteOffset(
                MemoryLayout.PathElement.groupElement("vector"));
        diskRecordEdgesCountOffset = diskCacheRecordLayout.byteOffset(
                MemoryLayout.PathElement.groupElement("edgesCount"));
        diskRecordEdgesOffset = diskCacheRecordLayout.byteOffset(
                MemoryLayout.PathElement.groupElement("edges"));


        if (this.vertexRecordSize > PAGE_SIZE_MULTIPLIER - 1) {
            this.pageSize = ((vertexRecordSize + PAGE_SIZE_MULTIPLIER - 1 - Integer.BYTES) /
                    (PAGE_SIZE_MULTIPLIER - Integer.BYTES)) * PAGE_SIZE_MULTIPLIER;
        } else {
            this.pageSize = PAGE_SIZE_MULTIPLIER;
        }


        this.verticesPerPage = (pageSize - Integer.BYTES) / vertexRecordSize;


        if (logger.isInfoEnabled()) {
            logger.info("Vector index " + name + " has been initialized. Vector lane count for distance calculation " +
                    "is " + SPECIES.length());
        }

        var pqParameters = PQ.calculatePQParameters(vectorDim, pqCompression);

        pqSubVectorSize = pqParameters.pqSubVectorSize;
        pqQuantizersCount = pqParameters.pqQuantizersCount;

        if (pqCompression % Float.BYTES != 0) {
            throw new IllegalArgumentException(
                    "Vector should be divided during creation of PQ codes without remainder.");
        }

        if (vectorDim % pqSubVectorSize != 0) {
            throw new IllegalArgumentException(
                    "Vector should be divided during creation of PQ codes without remainder.");
        }

        logger.info("PQ quantizers count is " + pqQuantizersCount + ", sub vector size is " + pqSubVectorSize +
                " elements , compression is " + pqCompression + " for index '" + name + "'");
        nearestGreedySearchCachedDataThreadLocal = ThreadLocal.withInitial(() -> new NearestGreedySearchCachedData(
                new IntOpenHashSet(8 * 1024,
                        Hash.VERY_FAST_LOAD_FACTOR), new float[pqQuantizersCount * (1 << Byte.SIZE)],
                new BoundedGreedyVertexPriorityQueue(maxAmountOfCandidates)));
    }


    public void buildIndex(int partitions, VectorReader vectorReader) {
        try {
            var memoryMXBean = ManagementFactory.getMemoryMXBean();
            logger.info("Building index for database {} ..., max non-heap memory usage {} Mb, committed {} Mb. " +
                            "Max heap memory usage {} Mb, committed {} Mb", name,
                    memoryMXBean.getNonHeapMemoryUsage().getMax() / 1024 / 1024,
                    memoryMXBean.getNonHeapMemoryUsage().getCommitted() / 1024 / 1024,
                    memoryMXBean.getHeapMemoryUsage().getMax() / 1024 / 1024,
                    memoryMXBean.getHeapMemoryUsage().getCommitted() / 1024 / 1024);

            if (vectorReader.size() == 0) {
                logger.info("Vector index " + name + ". There are no vectors to index. Stopping index build.");
                return;
            }

            logger.info("Generating PQ codes for vectors, non-heap memory usage {} Mb, heap memory usage {} Mb",
                    memoryMXBean.getNonHeapMemoryUsage().getUsed() / 1024 / 1024,
                    memoryMXBean.getHeapMemoryUsage().getUsed() / 1024 / 1024);

            var startPQ = System.nanoTime();
            var pqResult = PQ.generatePQCodes(pqQuantizersCount, pqSubVectorSize, distanceFunction, vectorReader, arena);

            pqCentroids = pqResult.pqCentroids;
            pqVectors = pqResult.pqVectors;
            pqCodeBaseSize = pqCentroids[0].length;

            var endPQ = System.nanoTime();
            logger.info("PQ codes for vectors have been generated. Time spent {} ms." +
                            " Non-heap memory usage {} Mb, heap memory usage {} Mb",
                    (endPQ - startPQ) / 1_000_000.0, memoryMXBean.getNonHeapMemoryUsage().getUsed() / 1024 / 1024,
                    memoryMXBean.getHeapMemoryUsage().getUsed() / 1024 / 1024);

            var size = vectorReader.size();

            logger.info("Calculation of graph search entry point ...");
            var startPQCentroid = System.nanoTime();

            var pqCentroid =
                    PQKMeans.calculatePartitions(pqCentroids, pqVectors, 1, 1, distanceFunction);
            var centroid = new float[vectorDim];
            for (int i = 0, pqCentroidVectorOffset = 0; i < pqQuantizersCount; i++,
                    pqCentroidVectorOffset += pqSubVectorSize) {
                var pqCentroidVector = Byte.toUnsignedInt(pqCentroid[i]);

                System.arraycopy(pqCentroids[i][pqCentroidVector], 0, centroid,
                        pqCentroidVectorOffset, pqSubVectorSize);
            }

            var endPQCentroid = System.nanoTime();
            logger.info("Calculation of graph search entry point has been finished. Time spent {} ms. " +
                            "Non-heap memory usage {} Mb, heap memory usage {} Mb",
                    (endPQCentroid - startPQCentroid) / 1_000_000.0,
                    memoryMXBean.getNonHeapMemoryUsage().getUsed() / 1024 / 1024,
                    memoryMXBean.getHeapMemoryUsage().getUsed() / 1024 / 1024);

            var medoidMindIndex = Integer.MAX_VALUE;
            var medoidMinDistance = Float.MAX_VALUE;

            logger.info("Splitting vectors into {} partitions...", partitions);
            var startPartition = System.nanoTime();
            var partitionsCentroids = PQKMeans.calculatePartitions(pqCentroids, pqVectors, partitions, 50,
                    distanceFunction);

            logger.info("Detection of {} partitions has been finished. " +
                            "Non-heap memory usage {} Mb, heap memory usage {} Mb", partitions,
                    memoryMXBean.getNonHeapMemoryUsage().getUsed() / 1024 / 1024,
                    memoryMXBean.getHeapMemoryUsage().getUsed() / 1024 / 1024);

            var cores = Runtime.getRuntime().availableProcessors();
            ArrayList<ExecutorService> vectorMutationThreads = new ArrayList<>(cores);


            logger.info("Using {} cores for processing of vectors", cores);

            for (var i = 0; i < cores; i++) {
                var id = i;
                vectorMutationThreads.add(Executors.newSingleThreadExecutor(r -> {
                    var thread = new Thread(r, name + "-vector mutator-" + id);
                    thread.setDaemon(true);
                    return thread;
                }));
            }

            var graphs = new MMapedGraph[partitions];

            IntArrayList[] vectorsByPartitions = new IntArrayList[partitions];
            for (int i = 0; i < partitions; i++) {
                vectorsByPartitions[i] = new IntArrayList(size / partitions);
            }
            var distanceTables = PQKMeans.distanceTables(pqCentroids, distanceFunction);
            for (int i = 0; i < size; i++) {
                var twoClosestClusters = PQKMeans.findTwoClosestClusters(pqVectors, i, partitionsCentroids,
                        distanceTables, pqQuantizersCount, pqCentroids[0].length);

                var firstPartition = (int) (twoClosestClusters >>> 32);
                var secondPartition = (int) twoClosestClusters;

                vectorsByPartitions[firstPartition].add(i);

                if (size == 1) {
                    if (firstPartition != secondPartition) {
                        vectorsByPartitions[secondPartition].add(i);
                    }
                } else {
                    assert firstPartition != secondPartition;
                    vectorsByPartitions[secondPartition].add(i);
                }

                if ((i & (1024 * 1024 - 1)) == 0) {
                    logger.info("Distribution of  {} vectors between partitions {} ({}%).", i, size, i * 100.0 / size);
                }
            }

            var graphFilePath = path.resolve(name + ".graph");
            try (var partitionsArena = Arena.openConfined()) {
                var totalPartitionsSize = 0;
                var maxPartitionSize = Integer.MIN_VALUE;
                var minPartitionSize = Integer.MAX_VALUE;

                var dmPartitions = new MemorySegment[partitions];

                for (var i = 0; i < partitions; i++) {
                    var partition = vectorsByPartitions[i];

                    var partitionSize = partition.size();
                    totalPartitionsSize += partitionSize;

                    if (partitionSize > maxPartitionSize) {
                        maxPartitionSize = partitionSize;
                    }
                    if (partitionSize < minPartitionSize) {
                        minPartitionSize = partitionSize;
                    }

                    dmPartitions[i] = partitionsArena.allocateArray(ValueLayout.JAVA_INT, partitionSize);
                    for (int j = 0; j < partitionSize; j++) {
                        dmPartitions[i].setAtIndex(ValueLayout.JAVA_INT, j, partition.getInt(j));
                    }
                }

                checkRequestedFreeSpace(path, size, totalPartitionsSize);

                var avgPartitionSize = totalPartitionsSize / partitions;
                var squareSum = 0;

                for (var i = 0; i < partitions; i++) {
                    var partition = dmPartitions[i];
                    var partitionSize = (int) (partition.byteSize() / Integer.SIZE);
                    squareSum += (partitionSize - avgPartitionSize) * (partitionSize - avgPartitionSize);
                }

                var endPartition = System.nanoTime();
                long maxPartitionSizeKBytes = calculateGraphPartitionSize(maxPartitionSize) / 1024;
                long minPartitionSizeKBytes = calculateGraphPartitionSize(minPartitionSize) / 1024;

                logger.info("Splitting vectors into {} partitions has been finished. Max. partition size {} vertexes " +
                                "({}Kb/{}Mb/{}Gb in memory), " +
                                "min partition size {} vertexes ({}Kb/{}Mb/{}Gb in memory), average size {}, deviation {}." +
                                " Time spent {} ms. Non-heap memory usage {} Mb, heap memory usage {} Mb.",
                        partitions, maxPartitionSize,
                        maxPartitionSizeKBytes, maxPartitionSizeKBytes / 1024, maxPartitionSizeKBytes / 1024 / 1024,
                        minPartitionSize,
                        minPartitionSizeKBytes, minPartitionSizeKBytes / 1024, minPartitionSizeKBytes / 1024 / 1024,
                        avgPartitionSize,
                        Math.sqrt((double) squareSum / partitions),
                        (endPartition - startPartition) / 1_000_000.0,
                        memoryMXBean.getNonHeapMemoryUsage().getUsed() / 1024 / 1024,
                        memoryMXBean.getHeapMemoryUsage().getUsed() / 1024 / 1024);
                logger.info("----------------------------------------------------------------------------------------------");
                logger.info("Distribution of vertices by partitions:");
                for (int i = 0; i < partitions; i++) {
                    logger.info("Partition {} has {} vectors.", i, (int) dmPartitions[i].byteSize() / Integer.BYTES);
                }
                logger.info("----------------------------------------------------------------------------------------------");

                if (Files.exists(graphFilePath)) {
                    logger.warn("File {} already exists and will be deleted.", path);
                    Files.delete(graphFilePath);
                }
                initFile(graphFilePath, size);

                var verticesProcessed = 0L;
                for (int i = 0; i < partitions; i++) {
                    var partition = dmPartitions[i];
                    var partitionSize = (int) partition.byteSize() / Integer.BYTES;

                    logger.info("Building search graph for partition {} with {} vectors...", i, partitionSize);
                    var graph = new MMapedGraph(partitionSize, i, name, path);
                    for (int j = 0; j < partitionSize; j++) {
                        var vectorIndex = partition.getAtIndex(ValueLayout.JAVA_INT, j);

                        var vector = vectorReader.read(vectorIndex);
                        graph.addVector(vectorIndex, vector);

                        var distance = Distance.computeDistance(vector, 0,
                                centroid, 0, vectorDim, distanceFunction);
                        if (distance < medoidMinDistance) {
                            medoidMinDistance = distance;
                            medoidMindIndex = vectorIndex;
                        }
                    }

                    graph.generateRandomEdges();

                    logger.info("Search graph for partition {} has been built. " +
                                    "Non-heap memory usage {} Mb, heap memory usage {} Mb. Pruning...", i,
                            memoryMXBean.getNonHeapMemoryUsage().getUsed() / 1024 / 1024,
                            memoryMXBean.getHeapMemoryUsage().getUsed() / 1024 / 1024);

                    var startPrune = System.nanoTime();
                    pruneIndex(graph, graph.medoid(), distanceMultiplication, vectorMutationThreads, i);
                    var endPrune = System.nanoTime();
                    logger.info("Search graph for partition {} has been pruned. Time spent {} ms.",
                            i, (endPrune - startPrune) / 1_000_000.0);

                    logger.info("Saving vectors of search graph for partition {} " +
                            "on disk under the path {} ...", i, graphFilePath.toAbsolutePath());

                    var startSave = System.nanoTime();

                    graph.sortVertexesByGlobalIndex();
                    graph.saveVectorsToDisk();

                    graph.clearEdgeVersions();

                    graph.convertLocalEdgesToGlobal();

                    verticesProcessed += partitionSize;
                    var endSave = System.nanoTime();

                    logger.info("Vectors of search graph for partition {} have been saved to the disk under the path {} " +
                                    "({}%). Time spent {} ms. " +
                                    "Non-heap memory usage {} Mb, heap memory usage {} Mb.",
                            i, graphFilePath.toAbsolutePath(), 100.0 * verticesProcessed / totalPartitionsSize,
                            (endSave - startSave) / 1_000_000.0,
                            memoryMXBean.getNonHeapMemoryUsage().getUsed() / 1024 / 1024,
                            memoryMXBean.getHeapMemoryUsage().getUsed() / 1024 / 1024);

                    graphs[i] = graph;
                }
            }


            logger.info("Merging and storing search graph partitions on disk under the path {} ...", graphFilePath.toAbsolutePath());
            var startSave = System.nanoTime();
            mergeAndStorePartitionsOnDisk(graphs, size);
            var endSave = System.nanoTime();
            logger.info("Search graph has been stored on disk under the path {}. Time spent {} ms. " +
                            "Non-heap memory usage {} Mb, heap memory usage {} Mb.",
                    graphFilePath.toAbsolutePath(), (endSave - startSave) / 1_000_000.0,
                    memoryMXBean.getNonHeapMemoryUsage().getUsed() / 1024 / 1024,
                    memoryMXBean.getHeapMemoryUsage().getUsed() / 1024 / 1024);

            for (var mutator : vectorMutationThreads) {
                mutator.shutdown();
            }
            vectorMutationThreads.clear();

            diskGraph = new DiskGraph(medoidMindIndex);
            verticesSize = size;

            storeIndexState();


        } catch (IOException e) {
            throw new RuntimeException("Error during creation of search graph for database " + name, e);
        } finally {
            try {
                vectorReader.close();
            } catch (Exception e) {
                logger.error("Error during closing of vector reader for database " + name, e);
            }
        }
    }

    public void loadIndex() {
        var medoid = readIndexState();
        var filePath = path.resolve(name + ".graph");

        try (var fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            diskCache = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), arena.scope());
            diskGraph = new DiskGraph(medoid);
        } catch (IOException e) {
            throw new RuntimeException("Error during reading of index data for database " + name, e);
        }
    }

    public void deleteIndex() {
        close();

        logger.info("Deleting index data for database {}...", name);
        var graphPath = path.resolve(name + ".graph");
        var dataFilePath = path.resolve(name + ".data");
        try {
            Files.deleteIfExists(graphPath);
            logger.info("File {} has been deleted.", graphPath);
            Files.deleteIfExists(dataFilePath);
            logger.info("File {} has been deleted.", dataFilePath);
        } catch (IOException e) {
            throw new RuntimeException("Error during deletion of index data for database " + name, e);
        }
        logger.info("Index data for database {} have been deleted.", name);
    }

    private void storeIndexState() throws IOException {
        var dataFilePath = path.resolve(name + ".data");
        try (var pqOutputStream = Files.newOutputStream(dataFilePath, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            try (var dataOutputStream = new DataOutputStream(new BufferedOutputStream(pqOutputStream))) {
                dataOutputStream.writeInt(diskGraph.medoid);
                dataOutputStream.writeInt(verticesSize);

                dataOutputStream.writeInt(pqQuantizersCount);
                dataOutputStream.writeInt(pqCodeBaseSize);
                dataOutputStream.writeInt(pqSubVectorSize);

                for (int i = 0; i < pqQuantizersCount; i++) {
                    for (int j = 0; j < pqCodeBaseSize; j++) {
                        for (int k = 0; k < pqSubVectorSize; k++) {
                            dataOutputStream.writeFloat(pqCentroids[i][j][k]);
                        }
                    }
                }

                var pqVectorsSize = pqVectors.byteSize();
                dataOutputStream.writeLong(pqVectorsSize);

                for (int i = 0; i < pqVectorsSize; i++) {
                    dataOutputStream.writeByte(pqVectors.get(ValueLayout.JAVA_BYTE, i));
                }

                dataOutputStream.flush();
            }
        }

        logger.info("Index data were loaded from disk for database {}", name);
    }

    private int readIndexState() {
        int medoid;
        var pqFilePath = path.resolve(name + ".data");
        try (var pqInputStream = Files.newInputStream(pqFilePath, StandardOpenOption.READ)) {
            try (var dataInputStream = new DataInputStream(pqInputStream)) {
                medoid = dataInputStream.readInt();
                verticesSize = dataInputStream.readInt();

                pqQuantizersCount = dataInputStream.readInt();
                pqCodeBaseSize = dataInputStream.readInt();
                pqSubVectorSize = dataInputStream.readInt();

                pqCentroids = new float[pqQuantizersCount][pqCodeBaseSize][pqSubVectorSize];
                for (int i = 0; i < pqQuantizersCount; i++) {
                    for (int j = 0; j < pqCodeBaseSize; j++) {
                        for (int k = 0; k < pqSubVectorSize; k++) {
                            pqCentroids[i][j][k] = dataInputStream.readFloat();
                        }
                    }
                }

                var pqVectorsSize = dataInputStream.readLong();
                pqVectors = arena.allocate(pqVectorsSize);
                for (int i = 0; i < pqVectorsSize; i++) {
                    pqVectors.set(ValueLayout.JAVA_BYTE, i, dataInputStream.readByte());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error during reading of index data for database " + name, e);
        }

        logger.info("Index data were loaded from disk for database {}", name);
        return medoid;
    }

    private void mergeAndStorePartitionsOnDisk(MMapedGraph[] partitions, int size) throws IOException {
        assert partitions.length > 0;

        if (partitions.length == 1) {
            return;
        }

        var completedPartitions = new boolean[partitions.length];
        for (var i = 0; i < partitions.length; i++) {
            var partition = partitions[i];

            if (partition.size == 0) {
                completedPartitions[i] = true;
            }
        }

        var edgeSet = new IntOpenHashSet(maxConnectionsPerVertex, Hash.FAST_LOAD_FACTOR);

        int resultIndex = 0;

        var heapGlobalIndexes = new LongHeapPriorityQueue(partitions.length,
                (globalIndex1, globalIndex2) -> Integer.compare((int) globalIndex1, (int) globalIndex2));

        var partitionsIndexes = new int[partitions.length];
        var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();

        for (int j = 0; j < partitions.length; j++) {
            addPartitionEdgeToHeap(completedPartitions, j,
                    heapGlobalIndexes, partitions[j], partitionsIndexes);
        }

        var logProgress = size > LOGGING_THRESHOLD;

        while (!heapGlobalIndexes.isEmpty()) {
            if (logProgress) {
                if ((resultIndex & (LOGGING_THRESHOLD - 1)) == 0) {
                    logger.info("Merging of vertices, {} vertices has been processed out of {} ({}%).",
                            resultIndex, size, resultIndex * 100.0 / size);
                }
            }

            var globalIndexPartitionIndex = heapGlobalIndexes.dequeueLong();

            var globalIndex = (int) (globalIndexPartitionIndex);
            var partitionIndex = (int) (globalIndexPartitionIndex >>> 32);
            var vertexIndexInsidePartition = partitionsIndexes[partitionIndex] - 1;
            var partition = partitions[partitionIndex];

            addPartitionEdgeToHeap(completedPartitions, partitionIndex,
                    heapGlobalIndexes, partition, partitionsIndexes);

            assert partition.globalIndexes.getAtIndex(ValueLayout.JAVA_INT,
                    vertexIndexInsidePartition) == globalIndex;

            assert resultIndex == globalIndex;
            var edgesOffset = (long) vertexIndexInsidePartition * (maxConnectionsPerVertex + 1) * Integer.BYTES;

            var localPageOffset = globalIndex % verticesPerPage;
            var pageOffset = (globalIndex / verticesPerPage) * pageSize;

            var recordOffset = (long) localPageOffset * vertexRecordSize + Integer.BYTES + pageOffset;

            var resultEdgesOffset = recordOffset + diskRecordEdgesOffset;
            var resultEdgesCountOffset = recordOffset + diskRecordEdgesCountOffset;

            if (heapGlobalIndexes.isEmpty() || globalIndex != (int) heapGlobalIndexes.firstLong()) {
                var edgesSize = partition.edges.get(ValueLayout.JAVA_INT, edgesOffset);
                assert edgesSize <= maxConnectionsPerVertex;

                edgesOffset += Integer.BYTES;
                diskCache.set(ValueLayout.JAVA_BYTE, resultEdgesCountOffset, (byte) edgesSize);

                MemorySegment.copy(partition.edges,
                        edgesOffset,
                        diskCache, resultEdgesOffset, (long) edgesSize * Integer.BYTES);
            } else {
                edgeSet.clear();

                var edgesCount = partition.edges.get(ValueLayout.JAVA_INT, edgesOffset);
                edgesOffset += Integer.BYTES;

                for (int n = 0; n < edgesCount; n++) {
                    var neighbour = partition.edges.get(ValueLayout.JAVA_INT, edgesOffset);
                    edgesOffset += Integer.BYTES;
                    edgeSet.add(neighbour);
                }

                do {
                    var nextGlobalIndexPartitionIndex = heapGlobalIndexes.dequeueLong();
                    assert globalIndex == (int) nextGlobalIndexPartitionIndex;

                    partitionIndex = (int) (nextGlobalIndexPartitionIndex >>> 32);
                    vertexIndexInsidePartition = partitionsIndexes[partitionIndex] - 1;
                    partition = partitions[partitionIndex];

                    addPartitionEdgeToHeap(completedPartitions, partitionIndex,
                            heapGlobalIndexes, partition, partitionsIndexes);

                    edgesOffset = (long) vertexIndexInsidePartition * (maxConnectionsPerVertex + 1) * Integer.BYTES;

                    edgesCount = partition.edges.get(ValueLayout.JAVA_INT, edgesOffset);
                    edgesOffset += Integer.BYTES;

                    for (int n = 0; n < edgesCount; n++) {
                        var neighbour = partition.edges.get(ValueLayout.JAVA_INT, edgesOffset);
                        edgesOffset += Integer.BYTES;

                        edgeSet.add(neighbour);
                    }
                } while (!heapGlobalIndexes.isEmpty() && globalIndex == (int) heapGlobalIndexes.firstLong());

                edgesCount = edgeSet.size();


                if (edgesCount > maxConnectionsPerVertex) {
                    diskCache.set(ValueLayout.JAVA_INT, resultEdgesCountOffset, maxConnectionsPerVertex);

                    var fullNeighbours = new int[edgesCount];
                    var edgesIterator = edgeSet.iterator();
                    for (int n = 0; n < edgesCount; n++) {
                        fullNeighbours[n] = edgesIterator.nextInt();
                    }

                    PermutationSampler.shuffle(rng, fullNeighbours);

                    assert resultEdgesCountOffset - resultEdgesOffset == 256;
                    for (int n = 0; n < maxConnectionsPerVertex; n++, resultEdgesOffset += Integer.BYTES) {
                        var neighbour = fullNeighbours[n];
                        diskCache.set(ValueLayout.JAVA_INT, resultEdgesOffset,
                                neighbour);
                    }
                } else {
                    diskCache.set(ValueLayout.JAVA_INT, resultEdgesCountOffset, edgesCount);

                    var edgesIterator = edgeSet.intIterator();
                    while (edgesIterator.hasNext()) {
                        var neighbour = edgesIterator.nextInt();
                        diskCache.set(ValueLayout.JAVA_INT, resultEdgesOffset,
                                neighbour);
                        resultEdgesOffset += Integer.BYTES;
                    }
                }
            }

            assert diskCache.get(ValueLayout.JAVA_BYTE, resultEdgesCountOffset) <= maxConnectionsPerVertex;

            resultIndex++;
        }

        for (var partition : partitions) {
            partition.close();
        }

        diskCache.force();

        if (logProgress) {
            logger.info("Merging of vertices has been finished.");
        }
    }

    private static void addPartitionEdgeToHeap(boolean[] completedPartitions, int partitionIndex,
                                               LongHeapPriorityQueue heapGlobalIndexes,
                                               MMapedGraph partition,
                                               int[] partitionsIndexes) {
        if (!completedPartitions[partitionIndex]) {
            heapGlobalIndexes.enqueue((((long) partitionIndex) << 32) | partition.globalIndexes.getAtIndex(ValueLayout.JAVA_INT,
                    partitionsIndexes[partitionIndex]));

            var newPartitionIndex = partitionsIndexes[partitionIndex] + 1;

            if (newPartitionIndex == partition.size) {
                completedPartitions[partitionIndex] = true;
            }

            partitionsIndexes[partitionIndex] = newPartitionIndex;
        }
    }


    public void resetPQErrorStat() {
        pqReCalculated = 0;
        pqReCalculationError = 0.0;
    }

    public double getPQErrorAvg() {
        return pqReCalculationError / pqReCalculated;
    }

    public void nearest(float[] vector, long[] result, int resultSize) {
        diskGraph.greedySearchNearest(vector, result,
                resultSize);
    }

    private void pruneIndex(MMapedGraph graph, int medoid, float distanceMultiplication,
                            final ArrayList<ExecutorService> vectorMutationThreads, int partitionId) {
        int size = graph.size;
        if (size == 0) {
            return;
        }

        var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
        try (var arena = Arena.openShared()) {
            var vectorIndexes = arena.allocateArray(ValueLayout.JAVA_INT, size);

            for (int i = 0; i < size; i++) {
                vectorIndexes.setAtIndex(ValueLayout.JAVA_INT, i, i);
            }
            permuteIndexes(vectorIndexes, rng, size);

            if (logger.isInfoEnabled()) {
                logger.info("Graph pruning started with distance multiplication " + distanceMultiplication + ".");
            }

            var mutatorFutures = new ArrayList<Future<?>>();

            var mutatorsCompleted = new AtomicInteger(0);
            var mutatorsCount = Math.min(vectorMutationThreads.size(), size);

            var neighborsArray = new ConcurrentLinkedQueue[mutatorsCount];


            for (int i = 0; i < mutatorsCount; i++) {
                //noinspection rawtypes
                neighborsArray[i] = new ConcurrentLinkedQueue();
            }

            for (var i = 0; i < mutatorsCount; i++) {
                var mutator = vectorMutationThreads.get(i);

                var mutatorId = i;
                var mutatorFuture = mutator.submit(() -> {
                    var index = 0;
                    while (true) {
                        @SuppressWarnings("unchecked")
                        var neighbourPairs = (ConcurrentLinkedQueue<IntIntImmutablePair>) neighborsArray[mutatorId];

                        if (!neighbourPairs.isEmpty()) {
                            var neighbourPair = neighbourPairs.poll();
                            do {
                                var vertexIndex = neighbourPair.leftInt();
                                var neighbourIndex = neighbourPair.rightInt();
                                var neighbours = graph.fetchNeighbours(vertexIndex);

                                if (!ArrayUtils.contains(neighbours, vertexIndex)) {
                                    if (graph.getNeighboursSize(vertexIndex) + 1 <= maxConnectionsPerVertex) {
                                        graph.acquireVertex(vertexIndex);
                                        try {
                                            graph.appendNeighbour(vertexIndex, neighbourIndex);
                                        } finally {
                                            graph.releaseVertex(vertexIndex);
                                        }
                                    } else {
                                        var neighbourSingleton = new Int2FloatOpenHashMap(1);
                                        neighbourSingleton.put(neighbourIndex, Float.NaN);
                                        graph.robustPrune(
                                                vertexIndex,
                                                neighbourSingleton,
                                                distanceMultiplication
                                        );
                                    }
                                }
                                neighbourPair = neighbourPairs.poll();
                            } while (neighbourPair != null);
                        } else if (mutatorsCompleted.get() == mutatorsCount) {
                            break;
                        }

                        if (index < size) {
                            var vectorIndex = vectorIndexes.getAtIndex(ValueLayout.JAVA_INT, index);
                            if (vectorIndex % mutatorsCount != mutatorId) {
                                index++;

                                if ((index & (1024 * 1024 - 1)) == 0) {
                                    logger.info("Graph pruning, partition {}: Thread # {}, {}% vertices were processed.",
                                            partitionId, mutatorId,
                                            (index * 100.0) / size);
                                }

                                continue;
                            }

                            graph.greedySearchPrune(medoid, vectorIndex);
                            var neighbourNeighbours = graph.fetchNeighbours(vectorIndex);
                            assert vectorIndex % mutatorsCount == mutatorId;

                            for (var neighbourIndex : neighbourNeighbours) {
                                var neighbourMutatorIndex = neighbourIndex % mutatorsCount;

                                @SuppressWarnings("unchecked")
                                var neighboursList =
                                        (ConcurrentLinkedQueue<IntIntImmutablePair>) neighborsArray[neighbourMutatorIndex];
                                neighboursList.add(new IntIntImmutablePair(neighbourIndex, vectorIndex));
                            }

                            index++;

                            if ((index & (1024 * 1024 - 1)) == 0) {
                                logger.info("Graph pruning, partition {}: Thread # {}, {}% vertices were processed.",
                                        partitionId, mutatorId,
                                        (index * 100.0) / size);
                            }
                        } else if (index == size) {
                            index = Integer.MAX_VALUE;
                            mutatorsCompleted.incrementAndGet();
                        }


                    }

                    logger.info("Graph pruning, partition {}: Thread # {} has completed.", partitionId, mutatorId);
                    return null;
                });
                mutatorFutures.add(mutatorFuture);
            }

            for (var mutatorFuture : mutatorFutures) {
                try {
                    mutatorFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info("Graph pruning: " + size + " vertices were processed.");
        }
    }

    private void permuteIndexes(MemorySegment indexes, UniformRandomProvider rng, int size) {
        for (int i = size; i > 1; i--) {
            var swapIndex = rng.nextInt(i);

            var firstValue = indexes.getAtIndex(ValueLayout.JAVA_INT, i - 1);
            var secondValue = indexes.getAtIndex(ValueLayout.JAVA_INT, swapIndex);

            indexes.setAtIndex(ValueLayout.JAVA_INT, i - 1, secondValue);
            indexes.setAtIndex(ValueLayout.JAVA_INT, swapIndex, firstValue);
        }
    }


    private float computeDistance(MemorySegment firstSegment, long firstSegmentFromOffset, MemorySegment secondSegment,
                                  long secondSegmentFromOffset, int size) {
        if (distanceFunction == L2_DISTANCE) {
            return L2Distance.computeL2Distance(firstSegment, firstSegmentFromOffset, secondSegment, secondSegmentFromOffset,
                    size);
        } else if (distanceFunction == DOT_DISTANCE) {
            return computeDotDistance(firstSegment, firstSegmentFromOffset, secondSegment, secondSegmentFromOffset,
                    size);
        } else {
            throw new IllegalStateException("Unknown distance function: " + distanceFunction);
        }
    }


    private float computeDistance(MemorySegment firstSegment, long firstSegmentFromOffset, float[] secondVector) {
        if (distanceFunction == L2_DISTANCE) {
            return L2Distance.computeL2Distance(firstSegment, firstSegmentFromOffset, secondVector,
                    0, secondVector.length);
        } else if (distanceFunction == DOT_DISTANCE) {
            return computeDotDistance(firstSegment, firstSegmentFromOffset, secondVector);
        } else {
            throw new IllegalStateException("Unknown distance function: " + distanceFunction);
        }
    }

    private void computeDistance(float[] originVector, @SuppressWarnings("SameParameterValue") int originVectorOffset,
                                 MemorySegment firstSegment,
                                 long firstSegmentFromOffset, MemorySegment secondSegment, long secondSegmentFromOffset,
                                 MemorySegment thirdSegment, long thirdSegmentFromOffset,
                                 MemorySegment fourthSegment, long fourthSegmentFromOffset,
                                 float[] result) {
        if (distanceFunction == L2_DISTANCE) {
            L2Distance.computeL2Distance(originVector, originVectorOffset, firstSegment, firstSegmentFromOffset,
                    secondSegment, secondSegmentFromOffset, thirdSegment, thirdSegmentFromOffset,
                    fourthSegment, fourthSegmentFromOffset, vectorDim, result);
        } else {
            throw new IllegalStateException("Unknown distance function: " + distanceFunction);
        }
    }


    static float computeDotDistance(MemorySegment firstSegment, long firstSegmentFromOffset, float[] secondVector) {
        var sumVector = FloatVector.zero(SPECIES);
        var index = 0;

        while (index < SPECIES.loopBound(secondVector.length)) {
            var first = FloatVector.fromMemorySegment(SPECIES, firstSegment,
                    firstSegmentFromOffset + (long) index * Float.BYTES, ByteOrder.nativeOrder());
            var second = FloatVector.fromArray(SPECIES, secondVector, index);

            sumVector = first.fma(second, sumVector);
            index += SPECIES.length();
        }

        var sum = sumVector.reduceLanes(VectorOperators.ADD);

        for (; index < secondVector.length; index++, firstSegmentFromOffset += Float.BYTES) {
            var mul = firstSegment.get(ValueLayout.JAVA_FLOAT, firstSegmentFromOffset)
                    * secondVector[index];
            sum += mul;
        }

        return -sum;
    }

    static float computeDotDistance(MemorySegment firstSegment, long firstSegmentFromOffset,
                                    MemorySegment secondSegment,
                                    long secondSegmentFromOffset, int size) {

        var sumVector = FloatVector.zero(SPECIES);
        var index = 0;

        while (index < SPECIES.loopBound(size)) {
            var first = FloatVector.fromMemorySegment(SPECIES, firstSegment,
                    firstSegmentFromOffset + (long) index * Float.BYTES,
                    ByteOrder.nativeOrder());
            var second = FloatVector.fromMemorySegment(SPECIES, secondSegment,
                    secondSegmentFromOffset + (long) index * Float.BYTES, ByteOrder.nativeOrder());

            sumVector = first.fma(second, sumVector);
            index += SPECIES.length();
        }

        var sum = sumVector.reduceLanes(VectorOperators.ADD);

        while (index < size) {
            var mul = firstSegment.get(ValueLayout.JAVA_FLOAT,
                    firstSegmentFromOffset + (long) index * Float.BYTES)
                    * secondSegment.get(ValueLayout.JAVA_FLOAT,
                    secondSegmentFromOffset + (long) index * Float.BYTES);
            sum += mul;
            index++;
        }

        return -sum;
    }

    private void initFile(Path path, int globalVertexCount) throws IOException {
        logger.info("Creating file {} for storing search graph...", path);
        var fileLength = calculateRequestedFileLength(globalVertexCount);
        logger.info("Vertices to be stored {}, vertices per page {}, page size {}, file length {}",
                globalVertexCount, verticesPerPage, pageSize, fileLength);

        try (var rwFile = new RandomAccessFile(path.toFile(), "rw")) {
            rwFile.setLength(fileLength);

            var channel = rwFile.getChannel();
            diskCache = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileLength, arena.scope());
        }
        logger.info("File {} for storing search graph has been created.", path);
    }

    private void checkRequestedFreeSpace(Path dbPath, int size, int totalPartitionsSize) throws IOException {
        var fileStore = Files.getFileStore(dbPath);
        var usableSpace = fileStore.getUsableSpace();
        var requiredGraphSpace = calculateRequestedFileLength(size);

        //space needed for mmap files to store edges and global indexes of all partitions.
        var requiredPartitionsSpace =
                (long) totalPartitionsSize * (maxConnectionsPerVertex + 1) * Integer.BYTES +
                        (long) totalPartitionsSize * Integer.BYTES;
        var requiredSpace = requiredGraphSpace + requiredPartitionsSpace;

        if (requiredSpace > usableSpace * 0.9) {
            throw new IllegalStateException("Not enough free space on disk. Required " + requiredSpace + " bytes, " +
                    "but only " + usableSpace +
                    " bytes are available. 10% of free space should be kept available on disk after index is built.");
        }
    }

    private long calculateGraphPartitionSize(long partitionSize) {
        //1. edges
        //2. global indexes
        //3. vertex records
        return partitionSize * (maxConnectionsPerVertex + 1) * Integer.BYTES +
                partitionSize * Integer.BYTES + partitionSize * vectorDim * Float.BYTES;
    }

    private long calculateRequestedFileLength(long globalVertexCount) {
        var pagesToWrite = (globalVertexCount + verticesPerPage - 1) / verticesPerPage;
        return pagesToWrite * pageSize;
    }


    @Override
    public void close() {
        if (!closed) {
            arena.close();
            closed = true;
        }

        diskGraph.close();
    }

    private final class MMapedGraph implements AutoCloseable {
        private int size = 0;

        private final MemorySegment edges;
        private final MemorySegment vectors;

        private final MemorySegment globalIndexes;

        @Nullable
        private AtomicLongArray edgeVersions;

        private final Arena edgesArena;

        private Arena vectorsArena;

        private int medoid = -1;

        private final String name;
        private final Path path;

        private final int id;

        private final long filesTs;

        private MMapedGraph(int capacity, int id, String name, Path path) throws IOException {
            this(capacity, false, id, name, path);
        }

        private MMapedGraph(int capacity, boolean skipVectors, int id, String name, Path path) throws IOException {
            this.edgeVersions = new AtomicLongArray(capacity);
            this.name = name;
            this.path = path;
            this.id = id;

            this.edgesArena = Arena.openShared();

            var edgesLayout = MemoryLayout.sequenceLayout((long) (maxConnectionsPerVertex + 1) * capacity,
                    ValueLayout.JAVA_INT);
            var globalIndexesLayout = MemoryLayout.sequenceLayout(capacity, ValueLayout.JAVA_INT);

            filesTs = System.nanoTime();


            var edgesPath = edgesPath(id, name, path, filesTs);
            logger.info("Partition {}, edges are going to be stored in file: {}", id, edgesPath);

            try (var edgesChannel = FileChannel.open(edgesPath(id, name, path, filesTs),
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
                this.edges = edgesChannel.map(FileChannel.MapMode.READ_WRITE, 0, edgesLayout.byteSize(),
                        edgesArena.scope());
            }

            var globalIndexesPath = globalIndexesPath(id, name, path, filesTs);
            logger.info("Partition {}, global indexes are going to be stored in file: {}", id, globalIndexesPath);

            try (var globalIndexesChannel = FileChannel.open(globalIndexesPath,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
                this.globalIndexes = globalIndexesChannel.map(FileChannel.MapMode.READ_WRITE, 0,
                        globalIndexesLayout.byteSize(),
                        edgesArena.scope());
            }

            if (!skipVectors) {
                this.vectorsArena = Arena.openShared();
                var vectorsLayout = MemoryLayout.sequenceLayout((long) capacity * vectorDim, ValueLayout.JAVA_FLOAT);
                this.vectors = vectorsArena.allocate(vectorsLayout);
            } else {
                vectors = null;
            }
        }

        @NotNull
        private static Path globalIndexesPath(int id, String name, Path path, long ts) {
            return path.resolve((name + "-" + id) + ts + ".globalIndexes");
        }

        @NotNull
        private static Path edgesPath(int id, String name, Path path, long ts) {
            return path.resolve((name + "-" + id) + ts + ".edges");
        }

        private void clearEdgeVersions() {
            edgeVersions = null;
        }

        private int medoid() {
            if (medoid == -1) {
                medoid = calculateMedoid();
            }

            return medoid;
        }

        private int calculateMedoid() {
            if (size == 1) {
                return 0;
            }

            var meanVector = new float[vectorDim];

            for (var i = 0; i < size; i++) {
                var vectorOffset = vectorOffset(i);
                for (var j = 0; j < vectorDim; j++) {
                    meanVector[j] += vectors.get(ValueLayout.JAVA_FLOAT, vectorOffset + (long) j * Float.BYTES);
                }
            }

            for (var j = 0; j < vectorDim; j++) {
                meanVector[j] = meanVector[j] / size;
            }

            var minDistance = Double.POSITIVE_INFINITY;
            var medoidIndex = -1;

            for (var i = 0; i < size; i++) {
                var distance = computeDistance(vectors, (long) i * vectorDim, meanVector);

                if (distance < minDistance) {
                    minDistance = distance;
                    medoidIndex = i;
                }
            }

            return medoidIndex;
        }


        private void addVector(int globalIndex, MemorySegment vector) {
            var index = (long) size * vectorDim;

            MemorySegment.copy(vector, 0, vectors,
                    index * Float.BYTES,
                    (long) vectorDim * Float.BYTES);
            globalIndexes.setAtIndex(ValueLayout.JAVA_INT, size, globalIndex);

            size++;
        }

        private void greedySearchPrune(
                int startVertexIndex,
                int vertexIndexToPrune) {
            var threadLocalCache = nearestGreedySearchCachedDataThreadLocal.get();
            var visitedVertexIndices = threadLocalCache.visistedVertexIndices;
            visitedVertexIndices.clear();

            var nearestCandidates = threadLocalCache.nearestCandidates;
            nearestCandidates.clear();

            var checkedVertices = new Int2FloatOpenHashMap(2 * maxAmountOfCandidates, Hash.FAST_LOAD_FACTOR);

            var startVectorOffset = vectorOffset(startVertexIndex);
            var queryVectorOffset = vectorOffset(vertexIndexToPrune);
            var dim = vectorDim;

            nearestCandidates.add(startVertexIndex, computeDistance(vectors, startVectorOffset,
                    vectors, queryVectorOffset, dim), false);

            var result = new float[4];
            var vectorsToCheck = new IntArrayList(4);

            while (true) {
                var notCheckedVertexPointer = nearestCandidates.nextNotCheckedVertexIndex();
                if (notCheckedVertexPointer < 0) {
                    break;
                }

                var currentVertexIndex = nearestCandidates.vertexIndex(notCheckedVertexPointer);
                assert nearestCandidates.size() <= maxAmountOfCandidates;

                checkedVertices.put(currentVertexIndex, nearestCandidates.vertexDistance(notCheckedVertexPointer));

                var vertexNeighbours = fetchNeighbours(currentVertexIndex);

                for (var vertexIndex : vertexNeighbours) {
                    if (visitedVertexIndices.add(vertexIndex)) {
                        vectorsToCheck.add(vertexIndex);
                        if (vectorsToCheck.size() == 4) {
                            var vertexIndexes = vectorsToCheck.elements();

                            var vectorOffset1 = vectorOffset(vertexIndexes[0]);
                            var vectorOffset2 = vectorOffset(vertexIndexes[1]);
                            var vectorOffset3 = vectorOffset(vertexIndexes[2]);
                            var vectorOffset4 = vectorOffset(vertexIndexes[3]);

                            Distance.computeDistance(vectors, queryVectorOffset, vectors, vectorOffset1,
                                    vectors, vectorOffset2, vectors, vectorOffset3, vectors, vectorOffset4,
                                    dim, result, distanceFunction
                            );

                            nearestCandidates.add(vertexIndexes[0], result[0], false);
                            nearestCandidates.add(vertexIndexes[1], result[1], false);
                            nearestCandidates.add(vertexIndexes[2], result[2], false);
                            nearestCandidates.add(vertexIndexes[3], result[3], false);

                            vectorsToCheck.clear();
                        }
                    }
                }

                var size = vectorsToCheck.size();
                if (size > 0) {
                    var vertexIndexes = vectorsToCheck.elements();
                    for (int i = 0; i < size; i++) {
                        var vertexIndex = vertexIndexes[i];
                        var vectorOffset = vectorOffset(vertexIndex);

                        var distance = computeDistance(vectors, queryVectorOffset, vectors, vectorOffset, dim);
                        nearestCandidates.add(vertexIndex, distance, false);
                    }
                    vectorsToCheck.clear();
                }
            }

            assert nearestCandidates.size() <= maxAmountOfCandidates;
            robustPrune(vertexIndexToPrune, checkedVertices, distanceMultiplication);
        }

        private void robustPrune(
                int vertexIndex,
                Int2FloatOpenHashMap neighboursCandidates,
                float distanceMultiplication
        ) {
            var dim = vectorDim;
            acquireVertex(vertexIndex);
            try {
                Int2FloatOpenHashMap candidates;
                if (getNeighboursSize(vertexIndex) > 0) {
                    var newCandidates = neighboursCandidates.clone();
                    for (var neighbourIndex : getNeighboursAndClear(vertexIndex)) {
                        newCandidates.putIfAbsent(neighbourIndex, Float.NaN);
                    }

                    candidates = newCandidates;
                } else {
                    candidates = neighboursCandidates;
                }

                var vectorOffset = vectorOffset(vertexIndex);

                var candidatesIterator = candidates.int2FloatEntrySet().fastIterator();
                var cachedCandidates = new TreeSet<RobustPruneVertex>();

                var vectorsToCalculate = new IntArrayList(4);
                var result = new float[4];

                while (candidatesIterator.hasNext()) {
                    var entry = candidatesIterator.next();
                    var candidateIndex = entry.getIntKey();
                    var distance = entry.getFloatValue();

                    if (Float.isNaN(distance)) {
                        vectorsToCalculate.add(candidateIndex);
                        if (vectorsToCalculate.size() == 4) {
                            var vectorIndexes = vectorsToCalculate.elements();

                            var vectorOffset1 = vectorOffset(vectorIndexes[0]);
                            var vectorOffset2 = vectorOffset(vectorIndexes[1]);
                            var vectorOffset3 = vectorOffset(vectorIndexes[2]);
                            var vectorOffset4 = vectorOffset(vectorIndexes[3]);

                            Distance.computeDistance(vectors, vectorOffset, vectors, vectorOffset1,
                                    vectors, vectorOffset2, vectors, vectorOffset3, vectors, vectorOffset4,
                                    dim, result, distanceFunction
                            );

                            cachedCandidates.add(new RobustPruneVertex(vectorIndexes[0], result[0]));
                            cachedCandidates.add(new RobustPruneVertex(vectorIndexes[1], result[1]));
                            cachedCandidates.add(new RobustPruneVertex(vectorIndexes[2], result[2]));
                            cachedCandidates.add(new RobustPruneVertex(vectorIndexes[3], result[3]));

                            vectorsToCalculate.clear();
                        }
                    } else {
                        var candidate = new RobustPruneVertex(candidateIndex, distance);
                        cachedCandidates.add(candidate);
                    }
                }

                if (!vectorsToCalculate.isEmpty()) {
                    var size = vectorsToCalculate.size();
                    var vectorIndexes = vectorsToCalculate.elements();
                    for (int i = 0; i < size; i++) {
                        var vectorIndex = vectorIndexes[i];

                        var vectorOff = vectorOffset(vectorIndex);
                        var distance = computeDistance(vectors, vectorOffset, vectors, vectorOff, dim);
                        cachedCandidates.add(new RobustPruneVertex(vectorIndex, distance));
                    }

                    vectorsToCalculate.clear();
                }

                var candidatesToCalculate = new ArrayList<RobustPruneVertex>(4);
                var removedCandidates = new ArrayList<RobustPruneVertex>(cachedCandidates.size());

                var neighbours = new IntArrayList(maxConnectionsPerVertex);
                var removed = new ArrayList<RobustPruneVertex>(cachedCandidates.size());

                var currentMultiplication = 1.0;
                neighboursLoop:
                while (currentMultiplication <= distanceMultiplication) {
                    if (!removed.isEmpty()) {
                        cachedCandidates.addAll(removed);
                        removed.clear();
                    }

                    while (!cachedCandidates.isEmpty()) {
                        var min = cachedCandidates.pollFirst();
                        assert min != null;
                        neighbours.add(min.index);

                        if (neighbours.size() == maxConnectionsPerVertex) {
                            break neighboursLoop;
                        }

                        var minIndex = vectorOffset(min.index);
                        for (RobustPruneVertex candidate : cachedCandidates) {
                            candidatesToCalculate.add(candidate);

                            assert candidatesToCalculate.size() <= 4;

                            if (candidatesToCalculate.size() == 4) {
                                var candidate1 = candidatesToCalculate.get(0);
                                var candidate2 = candidatesToCalculate.get(1);
                                var candidate3 = candidatesToCalculate.get(2);
                                var candidate4 = candidatesToCalculate.get(3);

                                var vectorOffset1 = vectorOffset(candidate1.index);
                                var vectorOffset2 = vectorOffset(candidate2.index);
                                var vectorOffset3 = vectorOffset(candidate3.index);
                                var vectorOffset4 = vectorOffset(candidate4.index);

                                Distance.computeDistance(vectors, minIndex, vectors, vectorOffset1,
                                        vectors, vectorOffset2, vectors, vectorOffset3,
                                        vectors, vectorOffset4, dim, result, distanceFunction);

                                if (result[0] * currentMultiplication <= candidate1.distance) {
                                    removedCandidates.add(candidate1);
                                }
                                if (result[1] * currentMultiplication <= candidate2.distance) {
                                    removedCandidates.add(candidate2);
                                }
                                if (result[2] * currentMultiplication <= candidate3.distance) {
                                    removedCandidates.add(candidate3);
                                }
                                if (result[3] * currentMultiplication <= candidate4.distance) {
                                    removedCandidates.add(candidate3);
                                }


                                candidatesToCalculate.clear();
                            }
                        }

                        if (candidatesToCalculate.size() > 1) {
                            for (RobustPruneVertex candidate : candidatesToCalculate) {
                                var distance = computeDistance(vectors, minIndex, vectors, vectorOffset(candidate.index), dim);
                                if (distance * currentMultiplication <= candidate.distance) {
                                    removedCandidates.add(candidate);
                                }
                            }
                            candidatesToCalculate.clear();
                        }

                        for (var removedCandidate : removedCandidates) {
                            cachedCandidates.remove(removedCandidate);
                        }

                        removed.addAll(removedCandidates);
                        removedCandidates.clear();
                    }

                    currentMultiplication *= 1.2;
                }

                var elements = neighbours.elements();
                var elementsSize = neighbours.size();

                ArrayUtils.reverse(elements, 0, elementsSize);

                setNeighbours(vertexIndex, elements, elementsSize);
            } finally {
                releaseVertex(vertexIndex);
            }
        }

        private long vectorOffset(int vertexIndex) {
            return (long) vertexIndex * vectorDim * Float.BYTES;
        }


        private int getNeighboursSize(int vertexIndex) {
            var edgeVersions = this.edgeVersions;
            assert edgeVersions != null;

            var version = edgeVersions.get(vertexIndex);
            while (true) {
                var size = edges.get(ValueLayout.JAVA_INT, edgesSizeOffset(vertexIndex));
                var newVersion = edgeVersions.get(vertexIndex);

                VarHandle.acquireFence();

                if (newVersion == version) {
                    assert size >= 0 && size <= maxConnectionsPerVertex;
                    return size;
                }

                version = newVersion;
            }
        }

        private long edgesSizeOffset(int vertexIndex) {
            return (long) vertexIndex * (maxConnectionsPerVertex + 1) * Integer.BYTES;
        }

        @NotNull
        private int[] fetchNeighbours(int vertexIndex) {
            var edgeVersions = this.edgeVersions;
            assert edgeVersions != null;

            var version = edgeVersions.get(vertexIndex);

            while (true) {
                var edgesIndex = (long) vertexIndex * (maxConnectionsPerVertex + 1);
                var size = edges.get(ValueLayout.JAVA_INT, edgesIndex * Integer.BYTES);

                var result = new int[size];
                MemorySegment.copy(edges, edgesIndex * Integer.BYTES + Integer.BYTES,
                        MemorySegment.ofArray(result), 0L, (long) size * Integer.BYTES);
                var newVersion = edgeVersions.get(vertexIndex);

                VarHandle.acquireFence();
                if (newVersion == version) {
                    assert size <= maxConnectionsPerVertex;
                    return result;
                }

                version = newVersion;
            }
        }


        private int fetchNeighboursNotThreadSafe(int vertexIndex, int[] neighbours) {
            var edgesIndex = (long) vertexIndex * (maxConnectionsPerVertex + 1);
            var size = edges.get(ValueLayout.JAVA_INT, edgesIndex * Integer.BYTES);

            MemorySegment.copy(edges, edgesIndex * Integer.BYTES + Integer.BYTES,
                    MemorySegment.ofArray(neighbours), 0L, (long) size * Integer.BYTES);
            assert size <= maxConnectionsPerVertex;

            return size;
        }

        private void fetchVectorNotThreadSafe(int vertexIndex, float[] vector) {
            var vectorOffset = vectorOffset(vertexIndex);
            MemorySegment.copy(vectors, vectorOffset, MemorySegment.ofArray(vector), 0L,
                    (long) vectorDim * Float.BYTES);
        }

        private void setVectorNotThreadSafe(int vertexIndex, float[] vector) {
            var vectorOffset = vectorOffset(vertexIndex);
            MemorySegment.copy(MemorySegment.ofArray(vector), 0L, vectors, vectorOffset,
                    (long) vectorDim * Float.BYTES);
        }

        private void setNeighboursNotThreadSafe(int vertexIndex, int[] neighbours, int size) {
            assert (size >= 0 && size <= maxConnectionsPerVertex);

            var edgesOffset = ((long) vertexIndex * (maxConnectionsPerVertex + 1)) * Integer.BYTES;
            edges.set(ValueLayout.JAVA_INT, edgesOffset, size);

            MemorySegment.copy(MemorySegment.ofArray(neighbours), 0L, edges,
                    edgesOffset + Integer.BYTES,
                    (long) size * Integer.BYTES);
        }

        private void setNeighbours(int vertexIndex, int[] neighbours, int size) {
            validateLocked(vertexIndex);
            assert (size >= 0 && size <= maxConnectionsPerVertex);

            var edgesOffset = ((long) vertexIndex * (maxConnectionsPerVertex + 1)) * Integer.BYTES;
            edges.set(ValueLayout.JAVA_INT, edgesOffset, size);

            MemorySegment.copy(MemorySegment.ofArray(neighbours), 0L, edges,
                    edgesOffset + Integer.BYTES,
                    (long) size * Integer.BYTES);
        }

        private void appendNeighbour(int vertexIndex, int neighbour) {
            validateLocked(vertexIndex);

            var edgesOffset = ((long) vertexIndex * (maxConnectionsPerVertex + 1)) * Integer.BYTES;
            var size = edges.get(ValueLayout.JAVA_INT, edgesOffset);

            assert size + 1 <= maxConnectionsPerVertex;

            edges.set(ValueLayout.JAVA_INT, edgesOffset, size + 1);
            edges.set(ValueLayout.JAVA_INT, edgesOffset + (long) (size + 1) * Integer.BYTES, neighbour);
        }


        private void generateRandomEdges() {
            if (size == 1) {
                return;
            }

            var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
            try (var arena = Arena.openConfined()) {
                var shuffledIndexes = arena.allocateArray(ValueLayout.JAVA_INT, size);

                for (var i = 0; i < size; i++) {
                    shuffledIndexes.setAtIndex(ValueLayout.JAVA_INT, i, i);
                }

                permuteIndexes(shuffledIndexes, rng, size);

                var maxEdges = Math.min(size - 1, maxConnectionsPerVertex);
                var shuffleIndex = 0;
                for (var i = 0; i < size; i++) {
                    var edgesOffset = edgesSizeOffset(i);
                    edges.set(ValueLayout.JAVA_INT, edgesOffset, maxEdges);

                    var addedEdges = 0;
                    while (addedEdges < maxEdges) {
                        var randomIndex = shuffledIndexes.getAtIndex(ValueLayout.JAVA_INT, shuffleIndex);
                        shuffleIndex++;

                        if (shuffleIndex == size) {
                            permuteIndexes(shuffledIndexes, rng, size);
                            shuffleIndex = 0;
                        } else if (randomIndex == i) {
                            continue;
                        }

                        edges.set(ValueLayout.JAVA_INT, edgesOffset + Integer.BYTES, randomIndex);
                        edgesOffset += Integer.BYTES;
                        addedEdges++;
                    }

                    if ((i & (1024 * 1024 - 1)) == 0) {
                        logger.info("Random graph generation: {} vertices out of {} ({}%) were processed.", i, size,
                                (i * 100.0) / size);
                    }
                }
            }

        }

        private int[] getNeighboursAndClear(int vertexIndex) {
            validateLocked(vertexIndex);
            var edgesOffset = ((long) vertexIndex * (maxConnectionsPerVertex + 1)) * Integer.BYTES;
            var result = fetchNeighbours(vertexIndex);

            var edgeVersions = this.edgeVersions;
            assert edgeVersions != null;

            edgeVersions.incrementAndGet(vertexIndex);
            edges.set(ValueLayout.JAVA_INT, edgesOffset, 0);
            edgeVersions.incrementAndGet(vertexIndex);

            return result;
        }

        private void acquireVertex(long vertexIndex) {
            var edgeVersions = this.edgeVersions;
            assert edgeVersions != null;

            while (true) {
                var version = edgeVersions.get((int) vertexIndex);
                if ((version & 1L) != 0L) {
                    throw new IllegalStateException("Vertex " + vertexIndex + " is already acquired");
                }
                if (edgeVersions.compareAndSet((int) vertexIndex, version, version + 1)) {
                    return;
                }
            }
        }

        private void validateLocked(long vertexIndex) {
            var edgeVersions = this.edgeVersions;
            assert edgeVersions != null;

            var version = edgeVersions.get((int) vertexIndex);
            if ((version & 1L) != 1L) {
                throw new IllegalStateException("Vertex " + vertexIndex + " is not acquired");
            }
        }

        private void releaseVertex(long vertexIndex) {
            var edgeVersions = this.edgeVersions;
            assert edgeVersions != null;

            while (true) {
                var version = edgeVersions.get((int) vertexIndex);
                if ((version & 1L) != 1L) {
                    throw new IllegalStateException("Vertex " + vertexIndex + " is not acquired");
                }
                if (edgeVersions.compareAndSet((int) vertexIndex, version, version + 1)) {
                    return;
                }
            }
        }

        private void saveVectorsToDisk() throws IOException {
            var verticesPerPage = pageSize / vertexRecordSize;
            var size = this.size;

            var loggingEnabled = size > LOGGING_THRESHOLD;
            if (loggingEnabled) {
                logger.info("Saving {} vertexes to disk", size);
            }

            for (long i = 0, vectorsIndex = 0; i < size; i++) {
                if (loggingEnabled && (i & (LOGGING_THRESHOLD - 1)) == 0) {
                    logger.info("Saving vertexes to disk: {} vertexes out of {} ({}%) were processed.", i, size,
                            (i * 100.0) / size);
                }

                var vertexGlobalIndex = globalIndexes.getAtIndex(ValueLayout.JAVA_INT, i);

                var localPageOffset = (long) vertexGlobalIndex % verticesPerPage;
                var pageOffset = ((long) vertexGlobalIndex / verticesPerPage) * pageSize;

                var recordOffset = localPageOffset * vertexRecordSize + Integer.BYTES + pageOffset;

                for (long j = 0; j < vectorDim; j++, vectorsIndex++) {
                    var vectorItem = vectors.get(ValueLayout.JAVA_FLOAT,
                            vectorsIndex * Float.BYTES);
                    var storedVectorItemOffset = recordOffset + diskRecordVectorsOffset + j * Float.BYTES;
                    var storedVectorItem = diskCache.get(ValueLayout.JAVA_FLOAT, storedVectorItemOffset);

                    //avoid unnecessary flushes to the disk
                    if (vectorItem != storedVectorItem) {
                        diskCache.set(ValueLayout.JAVA_FLOAT, storedVectorItemOffset, vectorItem);
                    }
                }
            }

            vectorsArena.close();

            vectorsArena = null;

            if (loggingEnabled) {
                logger.info("Saving vertexes to disk: all {} vertexes were processed.", size);
            }
        }

        private void convertLocalEdgesToGlobal() {
            var neighbours = new int[maxConnectionsPerVertex];
            for (int i = 0; i < size; i++) {
                var neighboursSize = fetchNeighboursNotThreadSafe(i, neighbours);

                for (int j = 0; j < neighboursSize; j++) {
                    var neighbour = neighbours[j];
                    var globalNeighbour = globalIndexes.getAtIndex(ValueLayout.JAVA_INT, neighbour);
                    neighbours[j] = globalNeighbour;
                }

                setNeighboursNotThreadSafe(i, neighbours, neighboursSize);
            }
        }

        private void sortVertexesByGlobalIndex() {
            var loggingEnabled = size > LOGGING_THRESHOLD;

            if (loggingEnabled) {
                logger.info("Sorting {} vertexes by global index", size);
            }

            var objectIndexes = new Integer[size];
            for (var i = 0; i < size; i++) {
                objectIndexes[i] = i;
            }

            Arrays.sort(objectIndexes,
                    Comparator.comparingInt((Integer i) -> globalIndexes.getAtIndex(ValueLayout.JAVA_INT, i)));

            var indexes = new int[size];
            for (var i = 0; i < size; i++) {
                indexes[i] = objectIndexes[i];
            }

            var invertedIndexesMap = new Int2IntOpenHashMap(size, Hash.FAST_LOAD_FACTOR);
            for (var i = 0; i < size; i++) {
                invertedIndexesMap.put(indexes[i], i);
            }

            var processedIndexes = new boolean[size];

            var neighboursToAssign = new int[maxConnectionsPerVertex];
            var tpmNeighboursToAssign = new int[maxConnectionsPerVertex];

            var vectorToAssign = new float[vectorDim];
            var tmpVectorToAssign = new float[vectorDim];

            for (int i = 0; i < size; i++) {
                if (loggingEnabled && (i & (LOGGING_THRESHOLD - 1)) == 0) {
                    logger.info("Sorting vertexes by global index: {} vertexes out of {} ({}%) were processed.", i, size,
                            (i * 100.0) / size);
                }

                if (!processedIndexes[i]) {
                    var currentIndexToProcess = i;
                    var indexToFetch = indexes[currentIndexToProcess];

                    var globalIndexToAssign = globalIndexes.getAtIndex(ValueLayout.JAVA_INT, indexToFetch);
                    var neighboursToAssignSize = fetchNeighboursNotThreadSafe(indexToFetch, neighboursToAssign);

                    fetchVectorNotThreadSafe(indexToFetch, vectorToAssign);

                    while (!processedIndexes[currentIndexToProcess]) {
                        int tmpNeighboursSize = fetchNeighboursNotThreadSafe(currentIndexToProcess, tpmNeighboursToAssign);
                        int tmpGlobalIndex = globalIndexes.getAtIndex(ValueLayout.JAVA_INT, currentIndexToProcess);

                        fetchVectorNotThreadSafe(currentIndexToProcess, tmpVectorToAssign);

                        globalIndexes.setAtIndex(ValueLayout.JAVA_INT, currentIndexToProcess, globalIndexToAssign);
                        setNeighboursNotThreadSafe(currentIndexToProcess, neighboursToAssign, neighboursToAssignSize);

                        setVectorNotThreadSafe(currentIndexToProcess, vectorToAssign);

                        var tmp = neighboursToAssign;
                        neighboursToAssign = tpmNeighboursToAssign;
                        tpmNeighboursToAssign = tmp;

                        var vecTmp = vectorToAssign;
                        vectorToAssign = tmpVectorToAssign;
                        tmpVectorToAssign = vecTmp;

                        neighboursToAssignSize = tmpNeighboursSize;
                        globalIndexToAssign = tmpGlobalIndex;

                        processedIndexes[currentIndexToProcess] = true;
                        currentIndexToProcess = invertedIndexesMap.get(currentIndexToProcess);
                    }
                }
            }

            if (loggingEnabled) {
                logger.info("Sorting vertexes by global index: all {} vertexes were processed.", size);
            }

        }

        @Override
        public void close() throws IOException {
            if (vectorsArena != null) {
                vectorsArena.close();
                vectorsArena = null;
            }

            edgesArena.close();
            var edgesPath = edgesPath(id, name, path, filesTs);
            var globalIndexesPath = globalIndexesPath(id, name, path, filesTs);

            Files.delete(edgesPath);
            logger.info("File {} is deleted", edgesPath);

            Files.delete(globalIndexesPath);
            logger.info("File {} is deleted", globalIndexesPath);
        }
    }

    private final class DiskGraph implements AutoCloseable {
        private final int medoid;

        private final BoundedCircularLongArrayQueue vertexPreloadRequests = new BoundedCircularLongArrayQueue(1024 * 1024);
        private final NonBlockingHashMapLongLong vertexPreloadRequestsMap = new NonBlockingHashMapLongLong(1024);
        private final ArrayList<ExecutorService> vectorPreloadThreads = new ArrayList<>(4);

        private DiskGraph(int medoid) {
            this.medoid = medoid;

            for (int i = 0; i < 4; i++) {
                var executorService = Executors.newSingleThreadExecutor(r -> {
                    var thread = new Thread(r, name + "-vector pre-loader");
                    thread.setDaemon(true);
                    return thread;
                });

                executorService.execute(new VertexPreloader());
                vectorPreloadThreads.add(executorService);
            }
        }

        private void greedySearchNearest(
                float[] queryVector,
                long[] result,
                int k
        ) {
            var requestedIssuePreloads = new IntOpenHashSet(maxAmountOfCandidates, Hash.FAST_LOAD_FACTOR);

            var threadLocalCache = nearestGreedySearchCachedDataThreadLocal.get();

            var visitedVertexIndices = threadLocalCache.visistedVertexIndices;
            visitedVertexIndices.clear();

            var nearestCandidates = threadLocalCache.nearestCandidates;
            nearestCandidates.clear();

            var startVertexIndex = medoid;
            var startVectorOffset = vectorOffset(startVertexIndex);

            var distanceResult = threadLocalCache.distanceResult;
            var vertexIndexesToCheck = threadLocalCache.vertexIndexesToCheck;
            vertexIndexesToCheck.clear();

            nearestCandidates.add(startVertexIndex, computeDistance(diskCache, startVectorOffset, queryVector),
                    false);

            assert nearestCandidates.size() <= maxAmountOfCandidates;
            visitedVertexIndices.add(startVertexIndex);

            float[] lookupTable = null;

            while (true) {
                int currentVertex = -1;

                vertexRecalculationLoop:
                while (true) {
                    vertexIndexesToCheck.clear();

                    while (vertexIndexesToCheck.size() < 4) {
                        var notCheckedVertex = nearestCandidates.nextNotCheckedVertexIndex();

                        if (notCheckedVertex < 0) {
                            if (vertexIndexesToCheck.isEmpty()) {
                                break vertexRecalculationLoop;
                            }

                            recalculateDistances(queryVector, nearestCandidates,
                                    vertexIndexesToCheck, distanceResult, requestedIssuePreloads);
                            continue;
                        }

                        if (nearestCandidates.isPqDistance(notCheckedVertex)) {
                            vertexIndexesToCheck.add(notCheckedVertex);
                            assert vertexIndexesToCheck.size() <= 4;
                        } else {
                            if (!vertexIndexesToCheck.isEmpty()) {
                                recalculateDistances(queryVector, nearestCandidates,
                                        vertexIndexesToCheck, distanceResult, requestedIssuePreloads);
                                continue;
                            }
                            currentVertex = nearestCandidates.vertexIndex(notCheckedVertex);
                            break vertexRecalculationLoop;
                        }
                    }
                    recalculateDistances(queryVector, nearestCandidates,
                            vertexIndexesToCheck, distanceResult, requestedIssuePreloads);
                }


                if (currentVertex < 0) {
                    break;
                }

                var recordOffset = recordOffset(currentVertex);
                var neighboursSizeOffset = recordOffset + diskRecordEdgesCountOffset;
                var neighboursCount = Byte.toUnsignedInt(diskCache.get(ValueLayout.JAVA_BYTE, neighboursSizeOffset));
                var neighboursEnd = neighboursCount * Integer.BYTES + diskRecordEdgesOffset + recordOffset;


                assert vertexIndexesToCheck.isEmpty();
                for (var neighboursOffset = recordOffset + diskRecordEdgesOffset;
                     neighboursOffset < neighboursEnd; neighboursOffset += Integer.BYTES) {
                    var vertexIndex = diskCache.get(ValueLayout.JAVA_INT, neighboursOffset);

                    if (visitedVertexIndices.add(vertexIndex)) {
                        if (lookupTable == null) {
                            lookupTable = threadLocalCache.lookupTable;
                            PQ.buildPQDistanceLookupTable(queryVector, lookupTable, pqCentroids, pqQuantizersCount,
                                    pqSubVectorSize, distanceFunction);
                        }

                        assert vertexIndexesToCheck.size() <= 4;

                        vertexIndexesToCheck.add(vertexIndex);
                        if (vertexIndexesToCheck.size() == 4) {
                            computePQDistances(lookupTable, vertexIndexesToCheck, nearestCandidates,
                                    distanceResult, requestedIssuePreloads);
                        }

                        assert vertexIndexesToCheck.size() <= 4;
                    }
                }

                assert vertexIndexesToCheck.size() <= 4;

                if (!vertexIndexesToCheck.isEmpty()) {
                    computePQDistances(lookupTable, vertexIndexesToCheck, nearestCandidates,
                            distanceResult, requestedIssuePreloads);
                }

                assert vertexIndexesToCheck.isEmpty();
                assert nearestCandidates.size() <= maxAmountOfCandidates;
            }

            var requestedVerticesIterator = requestedIssuePreloads.iterator();
            while (requestedVerticesIterator.hasNext()) {
                var vertexIndex = requestedVerticesIterator.nextInt();
                invalidatePreloadRequest(vertexIndex);
            }
            requestedIssuePreloads.clear();

            nearestCandidates.vertexIndices(result, k);
        }

        private void computePQDistance4Batch(float[] lookupTable, int vectorIndex1, int vectorIndex2,
                                             int vectorIndex3, int vectorIndex4, float[] result) {
            assert result.length == 4;

            var pqIndex1 = pqQuantizersCount * vectorIndex1;
            var pqIndex2 = pqQuantizersCount * vectorIndex2;
            var pqIndex3 = pqQuantizersCount * vectorIndex3;
            var pqIndex4 = pqQuantizersCount * vectorIndex4;

            var result1 = 0.0f;
            var result2 = 0.0f;
            var result3 = 0.0f;
            var result4 = 0.0f;

            for (int i = 0; i < pqQuantizersCount; i++) {
                var rowOffset = i * (1 << Byte.SIZE);

                var code1 = pqVectors.get(ValueLayout.JAVA_BYTE, pqIndex1 + i) & 0xFF;
                result1 += lookupTable[rowOffset + code1];

                var code2 = pqVectors.get(ValueLayout.JAVA_BYTE, pqIndex2 + i) & 0xFF;
                result2 += lookupTable[rowOffset + code2];

                var code3 = pqVectors.get(ValueLayout.JAVA_BYTE, pqIndex3 + i) & 0xFF;
                result3 += lookupTable[rowOffset + code3];

                var code4 = pqVectors.get(ValueLayout.JAVA_BYTE, pqIndex4 + i) & 0xFF;
                result4 += lookupTable[rowOffset + code4];
            }

            result[0] = result1;
            result[1] = result2;
            result[2] = result3;
            result[3] = result4;
        }

        private void computePQDistances(float[] lookupTable,
                                        IntArrayList vertexIndexesToCheck,
                                        BoundedGreedyVertexPriorityQueue nearestCandidates,
                                        float[] distanceResult, IntOpenHashSet requestedVerticesPreloads) {
            assert distanceResult.length == 4;
            assert vertexIndexesToCheck.size() <= 4;

            var elements = vertexIndexesToCheck.elements();
            var size = vertexIndexesToCheck.size();

            if (size < 4) {
                for (int i = 0; i < size; i++) {
                    var vertexIndex = elements[i];
                    var pqDistance = PQ.computePQDistance(pqVectors, lookupTable, vertexIndex, pqQuantizersCount);

                    addPqDistance(nearestCandidates, pqDistance, vertexIndex, requestedVerticesPreloads);
                }
            } else {
                var vertexIndex1 = elements[0];
                var vertexIndex2 = elements[1];
                var vertexIndex3 = elements[2];
                var vertexIndex4 = elements[3];

                computePQDistance4Batch(lookupTable, vertexIndex1, vertexIndex2, vertexIndex3, vertexIndex4,
                        distanceResult);


                for (int i = 0; i < 4; i++) {
                    var pqDistance = distanceResult[i];
                    var vertexIndex = elements[i];
                    addPqDistance(nearestCandidates, pqDistance, vertexIndex, requestedVerticesPreloads);
                }
            }

            vertexIndexesToCheck.clear();
        }

        private void addPqDistance(BoundedGreedyVertexPriorityQueue nearestCandidates, float pqDistance,
                                   int vertexIndex, IntOpenHashSet requestedVerticesPreloads) {
            if (nearestCandidates.size() < maxAmountOfCandidates) {
                nearestCandidates.add(vertexIndex, pqDistance, true);

                issueVertexPreloadRequest(vertexIndex);
                requestedVerticesPreloads.add(vertexIndex);
            } else {
                var lastVertexDistance = nearestCandidates.maxDistance();
                if (lastVertexDistance >= pqDistance) {
                    nearestCandidates.add(vertexIndex, pqDistance, true);
                    issueVertexPreloadRequest(vertexIndex);
                    requestedVerticesPreloads.add(vertexIndex);
                }
            }
        }

        private void issueVertexPreloadRequest(int vertexIndex) {
            vertexPreloadRequestsMap.putIfAbsent(vertexIndex, 0L);
            var requestCounter = vertexPreloadRequestsMap.get(vertexIndex);

            while (!vertexPreloadRequestsMap.replace(vertexIndex, requestCounter, requestCounter + 1)) {
                requestCounter = vertexPreloadRequestsMap.get(vertexIndex);
            }

            vertexPreloadRequests.add(vertexIndex);
        }


        private void recalculateDistances(float[] queryVector, BoundedGreedyVertexPriorityQueue nearestCandidates,
                                          IntArrayList vertexIndexesToCheck, float[] distanceResult,
                                          IntOpenHashSet requestedIssuePreloads) {
            var elements = vertexIndexesToCheck.elements();
            var size = vertexIndexesToCheck.size();

            if (size < 4) {
                for (int i = 0; i < size; i++) {
                    var notCheckedVertex = elements[i];

                    var vertexIndex = nearestCandidates.vertexIndex(notCheckedVertex);

                    invalidatePreloadRequest(vertexIndex, requestedIssuePreloads);

                    var preciseDistance = computeDistance(diskCache, vectorOffset(vertexIndex),
                            queryVector);
                    var pqDistance = nearestCandidates.vertexDistance(notCheckedVertex);
                    var newVertexIndex = nearestCandidates.resortVertex(notCheckedVertex, preciseDistance);

                    for (int k = i + 1; k < size; k++) {
                        elements[k] = elements[k] - ((elements[k] - newVertexIndex - 1) >>> (Integer.SIZE - 1));
                    }

                    if (preciseDistance != 0) {
                        pqReCalculated++;
                        pqReCalculationError += 100.0 * Math.abs(preciseDistance - pqDistance) / preciseDistance;
                    }
                }
            } else {
                var notCheckedVertex1 = elements[0];
                var notCheckedVertex2 = elements[1];
                var notCheckedVertex3 = elements[2];
                var notCheckedVertex4 = elements[3];

                var vertexIndex1 = nearestCandidates.vertexIndex(notCheckedVertex1);
                var vertexIndex2 = nearestCandidates.vertexIndex(notCheckedVertex2);
                var vertexIndex3 = nearestCandidates.vertexIndex(notCheckedVertex3);
                var vertexIndex4 = nearestCandidates.vertexIndex(notCheckedVertex4);

                assert notCheckedVertex1 < notCheckedVertex2;
                assert notCheckedVertex2 < notCheckedVertex3;
                assert notCheckedVertex3 < notCheckedVertex4;

                var vectorOffset1 = vectorOffset(vertexIndex1);
                var vectorOffset2 = vectorOffset(vertexIndex2);
                var vectorOffset3 = vectorOffset(vertexIndex3);
                var vectorOffset4 = vectorOffset(vertexIndex4);

                var pqDistance1 = nearestCandidates.vertexDistance(notCheckedVertex1);
                var pqDistance2 = nearestCandidates.vertexDistance(notCheckedVertex2);
                var pqDistance3 = nearestCandidates.vertexDistance(notCheckedVertex3);
                var pqDistance4 = nearestCandidates.vertexDistance(notCheckedVertex4);

                computeDistance(queryVector, 0, diskCache, vectorOffset1,
                        diskCache, vectorOffset2, diskCache, vectorOffset3, diskCache, vectorOffset4,
                        distanceResult);

                //preventing branch miss predictions using bit shift and subtraction
                var newVertexIndex1 = nearestCandidates.resortVertex(notCheckedVertex1, distanceResult[0]);
                assert vertexIndex1 == nearestCandidates.vertexIndex(newVertexIndex1);

                //if newVertexIndex1 >= notCheckedVertex1 then -1 else 0, the same logic
                //is applied for the rest follow-up indexes
                notCheckedVertex2 = notCheckedVertex2 -
                        ((notCheckedVertex2 - newVertexIndex1 - 1) >>> (Integer.SIZE - 1));
                notCheckedVertex3 = notCheckedVertex3 -
                        ((notCheckedVertex3 - newVertexIndex1 - 1) >>> (Integer.SIZE - 1));
                notCheckedVertex4 = notCheckedVertex4 -
                        ((notCheckedVertex4 - newVertexIndex1 - 1) >>> (Integer.SIZE - 1));
                assert vertexIndex2 == nearestCandidates.vertexIndex(notCheckedVertex2);
                assert vertexIndex3 == nearestCandidates.vertexIndex(notCheckedVertex3);
                assert vertexIndex4 == nearestCandidates.vertexIndex(notCheckedVertex4);

                var newVertexIndex2 = nearestCandidates.resortVertex(notCheckedVertex2, distanceResult[1]);
                assert vertexIndex2 == nearestCandidates.vertexIndex(newVertexIndex2);

                notCheckedVertex3 = notCheckedVertex3 - ((notCheckedVertex3 - newVertexIndex2 - 1) >>> (Integer.SIZE - 1));
                notCheckedVertex4 = notCheckedVertex4 - ((notCheckedVertex4 - newVertexIndex2 - 1) >>> (Integer.SIZE - 1));
                assert vertexIndex3 == nearestCandidates.vertexIndex(notCheckedVertex3);
                assert vertexIndex4 == nearestCandidates.vertexIndex(notCheckedVertex4);

                var newVertexIndex3 = nearestCandidates.resortVertex(notCheckedVertex3, distanceResult[2]);
                assert vertexIndex3 == nearestCandidates.vertexIndex(newVertexIndex3);

                notCheckedVertex4 = notCheckedVertex4 - ((notCheckedVertex4 - newVertexIndex3 - 1)
                        >>> (Integer.SIZE - 1));
                assert vertexIndex4 == nearestCandidates.vertexIndex(notCheckedVertex4);

                nearestCandidates.resortVertex(notCheckedVertex4, distanceResult[3]);

                if (distanceResult[0] != 0) {
                    pqReCalculated++;
                    pqReCalculationError += 100.0 * Math.abs(distanceResult[0] - pqDistance1) / distanceResult[0];
                }

                if (distanceResult[1] != 0) {
                    pqReCalculated++;
                    pqReCalculationError += 100.0 * Math.abs(distanceResult[1] - pqDistance2) / distanceResult[1];
                }

                if (distanceResult[2] != 0) {
                    pqReCalculated++;
                    pqReCalculationError += 100.0 * Math.abs(distanceResult[2] - pqDistance3) / distanceResult[2];
                }

                if (distanceResult[3] != 0) {
                    pqReCalculated++;
                    pqReCalculationError += 100.0 * Math.abs(distanceResult[3] - pqDistance4) / distanceResult[3];
                }

                invalidatePreloadRequest(vertexIndex1, requestedIssuePreloads);
                invalidatePreloadRequest(vertexIndex2, requestedIssuePreloads);
                invalidatePreloadRequest(vertexIndex3, requestedIssuePreloads);
                invalidatePreloadRequest(vertexIndex4, requestedIssuePreloads);
            }

            vertexIndexesToCheck.clear();
        }

        private void invalidatePreloadRequest(int vertexIndex, IntOpenHashSet requestedIssuePreloads) {
            invalidatePreloadRequest(vertexIndex);
            requestedIssuePreloads.remove(vertexIndex);
        }

        private void invalidatePreloadRequest(int vertexIndex) {
            var value = vertexPreloadRequestsMap.get(vertexIndex);
            assert value > 0;

            while (!vertexPreloadRequestsMap.replace(vertexIndex, value, value - 1)) {
                value = vertexPreloadRequestsMap.get(vertexIndex);
                assert value > 0;
            }
        }

        private long vectorOffset(long vertexIndex) {
            return recordOffset(vertexIndex) + diskRecordVectorsOffset;
        }

        private long recordOffset(long vertexIndex) {
            if (vertexIndex >= verticesSize) {
                throw new IllegalArgumentException();
            }

            var vertexPageIndex = vertexIndex / verticesPerPage;
            var vertexPageOffset = vertexPageIndex * pageSize;
            var vertexOffset = (vertexIndex % verticesPerPage) * vertexRecordSize + Integer.BYTES;

            return vertexPageOffset + vertexOffset;
        }

        public void close() {
            vertexPreloadRequests.stop();

            for (var vectorPreloadThread : vectorPreloadThreads) {
                vectorPreloadThread.shutdown();
            }
        }

        private final class VertexPreloader implements Runnable {
            @Override
            public void run() {
                try {
                    while (true) {
                        var vertexIndex = vertexPreloadRequests.dequeue();
                        if (vertexIndex < 0) {
                            return;
                        }

                        var requestCounter = vertexPreloadRequestsMap.get(vertexIndex);
                        if (requestCounter > 0) {
                            var vectorOffset = vectorOffset(vertexIndex);

                            for (long i = vectorOffset;
                                 i < vectorOffset + (long) vectorDim * Float.BYTES; i += Float.BYTES) {
                                diskCache.get(ValueLayout.JAVA_FLOAT, i);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static final class NearestGreedySearchCachedData {
        private final IntOpenHashSet visistedVertexIndices;
        private final float[] lookupTable;

        private final BoundedGreedyVertexPriorityQueue nearestCandidates;

        private final float[] distanceResult;

        private final IntArrayList vertexIndexesToCheck = new IntArrayList();


        private NearestGreedySearchCachedData(IntOpenHashSet vertexIndices, float[] lookupTable, BoundedGreedyVertexPriorityQueue nearestCandidates) {
            this.visistedVertexIndices = vertexIndices;
            this.lookupTable = lookupTable;
            this.nearestCandidates = nearestCandidates;
            this.distanceResult = new float[4];
        }
    }
}