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
package jetbrains.vectoriadb.index;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIntImmutablePair;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongHeapPriorityQueue;
import jetbrains.vectoriadb.index.diskcache.DiskCache;
import jetbrains.vectoriadb.index.util.collections.BoundedGreedyVertexPriorityQueue;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.sampling.PermutationSampler;
import org.apache.commons.rng.simple.RandomSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

public final class IndexBuilder {
    public static final int DEFAULT_MAX_CONNECTIONS_PER_VERTEX = 128;
    public static final int DEFAULT_MAX_AMOUNT_OF_CANDIDATES = 128;
    public static final float DEFAULT_DISTANCE_MULTIPLICATION = 2.0f;
    public static final int DEFAULT_COMPRESSION_RATIO = 32;

    private static final Logger logger = LoggerFactory.getLogger(IndexBuilder.class);

    public static void buildIndex(String name, int vectorsDimension,
                                  Path indexPath, Path dataPath, long graphPartitionMemoryConsumption,
                                  Distance distance, ProgressTracker progressTracker) throws IOException {
        buildIndex(name, vectorsDimension, DEFAULT_COMPRESSION_RATIO,
                DEFAULT_DISTANCE_MULTIPLICATION, indexPath, dataPath,
                graphPartitionMemoryConsumption, DEFAULT_MAX_CONNECTIONS_PER_VERTEX,
                DEFAULT_MAX_AMOUNT_OF_CANDIDATES, distance,
                progressTracker);
    }

    public static void buildIndex(String name, int vectorsDimension, int compressionRatio,
                                  float distanceMultiplication,
                                  Path indexDirectoryPath, Path dataStoreFilePath, long memoryConsumption,
                                  int maxConnectionsPerVertex,
                                  int maxAmountOfCandidates,
                                  Distance distance, ProgressTracker progressTracker) throws IOException {
        if (progressTracker == null) {
            progressTracker = new NoOpProgressTracker();
        }

        progressTracker.pushPhase("index building");
        try {
            var pageStructure = DiskCache.createPageStructure(vectorsDimension, maxConnectionsPerVertex);

            var pageSize = pageStructure.pageSize();
            var verticesCountPerPage = pageStructure.verticesCountPerPage();
            var vertexRecordSize = pageStructure.vertexRecordSize();
            var recordVectorsOffset = pageStructure.recordVectorsOffset();
            var recordEdgesOffset = pageStructure.recordEdgesOffset();
            var recordEdgesCountOffset = pageStructure.recordEdgesCountOffset();

            try (var vectorReader = new MmapVectorReader(vectorsDimension, dataStoreFilePath)) {
                try (var arena = Arena.openShared()) {

                    var size = vectorReader.size();
                    if (size == 0) {
                        logger.info("Vector index " + name + ". There are no vectors to index. Stopping index build.");
                        return;
                    }

                    var quantizer = distance.quantizer();
                    var distanceFunction = distance.buildDistanceFunction();

                    quantizer.generatePQCodes(vectorsDimension, compressionRatio, vectorReader, progressTracker);

                    progressTracker.pushPhase("Calculating graph search entry point");
                    float[] centroid;
                    try {
                        centroid = quantizer.calculateCentroids(1, 50,
                                distanceFunction, progressTracker)[0];
                    } finally {
                        progressTracker.pullPhase();
                    }


                    var medoidMinIndex = Integer.MAX_VALUE;
                    var medoidMinDistance = Float.MAX_VALUE;

                    var cores = Runtime.getRuntime().availableProcessors();
                    ArrayList<ExecutorService> vectorMutationThreads = new ArrayList<>(cores);

                    for (var i = 0; i < cores; i++) {
                        var id = i;
                        vectorMutationThreads.add(Executors.newSingleThreadExecutor(r -> {
                            var thread = new Thread(r, name + "-vector mutator-" + id);
                            thread.setDaemon(true);
                            return thread;
                        }));
                    }

                    var verticesCount = vectorReader.size();

                    var partitions =
                            (int) Math.max(1,
                                    3 * calculateGraphPartitionSize(2L * verticesCount,
                                            maxConnectionsPerVertex, vectorsDimension) / memoryConsumption);

                    var totalPartitionsSize = 0;
                    IntArrayList[] vectorsByPartitions;
                    var splitIteration = 0;
                    while (true) {
                        splitIteration++;
                        progressTracker.pushPhase("splitting vectors by partitions, iteration " + splitIteration);
                        try {
                            var startPartition = System.nanoTime();
                            vectorsByPartitions = quantizer.splitVectorsByPartitions(partitions, 50,
                                    distanceFunction, progressTracker);

                            totalPartitionsSize = 0;
                            var maxPartitionSize = Integer.MIN_VALUE;
                            var minPartitionSize = Integer.MAX_VALUE;

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
                            }

                            checkRequestedFreeSpace(indexDirectoryPath, size, totalPartitionsSize, maxConnectionsPerVertex,
                                    pageSize, verticesCountPerPage);

                            var avgPartitionSize = totalPartitionsSize / partitions;
                            var squareSum = 0L;

                            for (var i = 0; i < partitions; i++) {
                                var partition = vectorsByPartitions[i];
                                var partitionSize = partition.size();
                                squareSum += (long) (partitionSize - avgPartitionSize) * (partitionSize - avgPartitionSize);
                            }

                            var endPartition = System.nanoTime();
                            var maxPartitionSizeBytes = calculateGraphPartitionSize(maxPartitionSize, maxConnectionsPerVertex,
                                    vectorsDimension);
                            long maxPartitionSizeKBytes = maxPartitionSizeBytes / 1024;
                            long minPartitionSizeKBytes =
                                    calculateGraphPartitionSize(minPartitionSize, maxConnectionsPerVertex, vectorsDimension) / 1024;

                            //noinspection IntegerDivisionInFloatingPointContext
                            logger.info("Splitting vectors into {} partitions has been finished. Max. partition size {} vertexes " +
                                            "({}Kb/{}Mb/{}Gb in memory), " +
                                            "min partition size {} vertexes ({}Kb/{}Mb/{}Gb in memory), average size {}, deviation {}." +
                                            " Time spent {} ms.",
                                    partitions, maxPartitionSize,
                                    maxPartitionSizeKBytes, maxPartitionSizeKBytes / 1024, maxPartitionSizeKBytes / 1024 / 1024,
                                    minPartitionSize,
                                    minPartitionSizeKBytes, minPartitionSizeKBytes / 1024, minPartitionSizeKBytes / 1024 / 1024,
                                    avgPartitionSize,
                                    Math.sqrt(squareSum / partitions),
                                    (endPartition - startPartition) / 1_000_000.0);
                            logger.info("----------------------------------------------------------------------------------------------");

                            if (maxPartitionSize > memoryConsumption) {
                                partitions = (int) (1.2 * partitions);
                                logger.info("Max partition size {} bytes is greater than requested memory consumption {} bytes. " +
                                                "Trying to split vectors into {} partitions...", maxPartitionSizeBytes, memoryConsumption,
                                        partitions);
                                continue;
                            }

                            break;
                        } finally {
                            progressTracker.pullPhase();
                        }
                    }

                    try (var partitionsArena = Arena.openConfined()) {
                        var dmPartitions = new MemorySegment[partitions];

                        for (var i = 0; i < partitions; i++) {
                            var partition = vectorsByPartitions[i];
                            var partitionSize = partition.size();

                            dmPartitions[i] = partitionsArena.allocateArray(ValueLayout.JAVA_INT, partitionSize);
                            for (int j = 0; j < partitionSize; j++) {
                                dmPartitions[i].setAtIndex(ValueLayout.JAVA_INT, j, partition.getInt(j));
                            }
                        }

                        logger.info("Distribution of vertices by partitions:");
                        for (int i = 0; i < partitions; i++) {
                            logger.info("Partition {} has {} vectors.", i, (int) dmPartitions[i].byteSize() / Integer.BYTES);
                        }
                        logger.info("----------------------------------------------------------------------------------------------");

                        var graphFilePath = indexDirectoryPath.resolve(name + ".graph");
                        if (Files.exists(graphFilePath)) {
                            logger.warn("File {} already exists and will be deleted.", graphFilePath);
                            Files.delete(graphFilePath);
                        }

                        var diskCache = initFile(graphFilePath, size, verticesCountPerPage,
                                pageSize, arena);

                        var graphs = new MMapedGraph[partitions];
                        var verticesProcessed = 0L;

                        progressTracker.pushPhase("building search graph partitions");
                        try {
                            for (int i = 0; i < partitions; i++) {
                                if (progressTracker.isProgressUpdatedRequired()) {
                                    progressTracker.progress((double) 100 * verticesProcessed / totalPartitionsSize);
                                }

                                var partition = dmPartitions[i];
                                var partitionSize = (int) partition.byteSize() / Integer.BYTES;

                                var graph = new MMapedGraph(partitionSize, i, name, indexDirectoryPath, maxConnectionsPerVertex,
                                        vectorsDimension, distanceFunction, maxAmountOfCandidates, pageSize,
                                        vertexRecordSize, recordVectorsOffset, diskCache);
                                progressTracker.pushPhase("building search graph for partition " + i,
                                        "partition size", String.valueOf(partitionSize));
                                try {
                                    for (int j = 0; j < partitionSize; j++) {
                                        var vectorIndex = partition.getAtIndex(ValueLayout.JAVA_INT, j);

                                        var vector = vectorReader.read(vectorIndex);
                                        graph.addVector(vectorIndex, vector);

                                        var currentDistance = distanceFunction.computeDistance(vector, 0, centroid,
                                                0, vectorsDimension);
                                        if (currentDistance < medoidMinDistance) {
                                            medoidMinDistance = currentDistance;
                                            medoidMinIndex = vectorIndex;
                                        }
                                    }
                                } finally {
                                    progressTracker.pullPhase();
                                }

                                progressTracker.pushPhase("generation of random edges for partition " + i);
                                try {
                                    graph.generateRandomEdges(progressTracker);
                                } finally {
                                    progressTracker.pullPhase();
                                }

                                progressTracker.pushPhase("pruning search graph for partition " + i);
                                try {
                                    pruneIndex(graph, graph.medoid(), distanceMultiplication, vectorMutationThreads,
                                            maxConnectionsPerVertex, maxAmountOfCandidates, progressTracker);
                                } finally {
                                    progressTracker.pullPhase();
                                }

                                progressTracker.
                                        pushPhase("saving vectors of search graph for partition " + i);
                                try {
                                    graph.sortVertexesByGlobalIndex();
                                    graph.saveVectorsToDisk();

                                    graph.clearEdgeVersions();

                                    graph.convertLocalEdgesToGlobal();

                                    verticesProcessed += partitionSize;
                                } finally {
                                    progressTracker.pullPhase();
                                }

                                graphs[i] = graph;
                            }
                        } finally {
                            progressTracker.pullPhase();
                        }

                        progressTracker.pushPhase("merging search graph partitions");
                        try {
                            mergeAndStorePartitionsOnDisk(graphs, maxConnectionsPerVertex, verticesCountPerPage,
                                    pageSize, vertexRecordSize, recordEdgesOffset, recordEdgesCountOffset
                                    , diskCache);
                        } finally {
                            progressTracker.pullPhase();
                        }

                        for (var mutator : vectorMutationThreads) {
                            mutator.shutdown();
                        }
                        vectorMutationThreads.clear();

                        storeIndexState(medoidMinIndex, vectorReader.size(), quantizer, name, indexDirectoryPath);
                    }
                }
            }

            Files.deleteIfExists(dataStoreFilePath);
        } finally {
            progressTracker.pullPhase();
        }
    }

    private static void storeIndexState(int medoid, int verticesSize, Quantizer quantizer,
                                        String name, Path indexPath) throws IOException {
        var dataFilePath = indexPath.resolve(name + ".data");
        try (var pqOutputStream = Files.newOutputStream(dataFilePath, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            try (var dataOutputStream = new DataOutputStream(new BufferedOutputStream(pqOutputStream))) {
                dataOutputStream.writeInt(medoid);
                dataOutputStream.writeInt(verticesSize);

                quantizer.store(dataOutputStream);
                dataOutputStream.flush();
            }
        }
    }


    private static void mergeAndStorePartitionsOnDisk(MMapedGraph[] partitions, int maxConnectionsPerVertex,
                                                      int verticesPerPage, int pageSize, int vertexRecordSize,
                                                      int diskRecordEdgesOffset, int diskRecordEdgesCountOffset,
                                                      MemorySegment diskCache) throws IOException {
        assert partitions.length > 0;

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


        while (!heapGlobalIndexes.isEmpty()) {
            var globalIndexPartitionIndex = heapGlobalIndexes.dequeueLong();

            var globalIndex = (long) ((int) (globalIndexPartitionIndex));
            var partitionIndex = (int) (globalIndexPartitionIndex >>> 32);
            var vertexIndexInsidePartition = partitionsIndexes[partitionIndex] - 1;
            var partition = partitions[partitionIndex];

            addPartitionEdgeToHeap(completedPartitions, partitionIndex,
                    heapGlobalIndexes, partition, partitionsIndexes);

            assert resultIndex == globalIndex;
            var edgesOffset = (long) vertexIndexInsidePartition * (maxConnectionsPerVertex + 1) * Integer.BYTES;

            var localPageOffset = globalIndex % verticesPerPage;
            var pageOffset = (globalIndex / verticesPerPage) * pageSize;

            var recordOffset = localPageOffset * vertexRecordSize + Long.BYTES + pageOffset;

            var resultEdgesOffset = recordOffset + diskRecordEdgesOffset;
            var resultEdgesCountOffset = recordOffset + diskRecordEdgesCountOffset;

            if (heapGlobalIndexes.isEmpty() || globalIndex != (int) heapGlobalIndexes.firstLong()) {
                var edgesSize = (long) partition.edges.get(ValueLayout.JAVA_INT, edgesOffset);
                assert edgesSize <= maxConnectionsPerVertex;

                edgesOffset += Integer.BYTES;
                diskCache.set(ValueLayout.JAVA_INT, resultEdgesCountOffset, (byte) edgesSize);

                MemorySegment.copy(partition.edges,
                        edgesOffset,
                        diskCache, resultEdgesOffset, edgesSize * Integer.BYTES);
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
                } while (!heapGlobalIndexes.isEmpty() && globalIndexPartitionIndex == (int) heapGlobalIndexes.firstLong());

                edgesCount = edgeSet.size();

                if (edgesCount > maxConnectionsPerVertex) {
                    diskCache.set(ValueLayout.JAVA_INT, resultEdgesCountOffset, maxConnectionsPerVertex);

                    var fullNeighbours = new int[edgesCount];
                    var edgesIterator = edgeSet.iterator();
                    for (int n = 0; n < edgesCount; n++) {
                        fullNeighbours[n] = edgesIterator.nextInt();
                    }

                    PermutationSampler.shuffle(rng, fullNeighbours);

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
    }

    private static void addPartitionEdgeToHeap(boolean[] completedPartitions, int partitionIndex,
                                               LongHeapPriorityQueue heapGlobalIndexes,
                                               MMapedGraph partition,
                                               int[] partitionsIndexes) {
        if (!completedPartitions[partitionIndex]) {
            heapGlobalIndexes.enqueue((((long) partitionIndex) << 32) |
                    partition.globalIndexes.getAtIndex(ValueLayout.JAVA_INT,
                            partitionsIndexes[partitionIndex]));

            var newPartitionIndex = partitionsIndexes[partitionIndex] + 1;

            if (newPartitionIndex == partition.size) {
                completedPartitions[partitionIndex] = true;
            }

            partitionsIndexes[partitionIndex] = newPartitionIndex;
        }
    }

    private static void pruneIndex(MMapedGraph graph, int medoid, float distanceMultiplication,
                                   final ArrayList<ExecutorService> vectorMutationThreads,
                                   int maxConnectionsPerVertex, int maxAmountOfCandidates,
                                   @NotNull ProgressTracker progressTracker) {
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

            var mutatorFutures = new ArrayList<Future<?>>();
            var mutatorsCompleted = new AtomicInteger(0);
            var mutatorsCount = Math.min(vectorMutationThreads.size(), size);
            var neighborsArray = new ConcurrentLinkedQueue[mutatorsCount];

            for (int i = 0; i < mutatorsCount; i++) {
                //noinspection rawtypes
                neighborsArray[i] = new ConcurrentLinkedQueue();
            }

            var mtProgressTracker = new BoundedMTProgressTrackerFactory(mutatorsCount, progressTracker);
            for (var i = 0; i < mutatorsCount; i++) {
                var mutator = vectorMutationThreads.get(i);

                var mutatorId = i;
                var mutatorFuture = mutator.submit(() -> {
                    try (var localProgressTracker = mtProgressTracker.createThreadLocalTracker(mutatorId)) {
                        var index = 0;
                        var visitedVertices = new IntOpenHashSet(8 * 1024, Hash.VERY_FAST_LOAD_FACTOR);
                        var nearestCandidates = new BoundedGreedyVertexPriorityQueue(maxAmountOfCandidates);
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
                                    localProgressTracker.progress((index * 100.0) / size);

                                    continue;
                                }

                                graph.greedySearchPrune(medoid, vectorIndex, visitedVertices, nearestCandidates,
                                        distanceMultiplication);

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
                                localProgressTracker.progress((index * 100.0) / size);
                            } else if (index == size) {
                                index = Integer.MAX_VALUE;
                                mutatorsCompleted.incrementAndGet();
                            }


                        }
                        return null;
                    }
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
    }

    private static void permuteIndexes(MemorySegment indexes, UniformRandomProvider rng, int size) {
        for (int i = size; i > 1; i--) {
            var swapIndex = rng.nextInt(i);

            var firstValue = indexes.getAtIndex(ValueLayout.JAVA_INT, i - 1);
            var secondValue = indexes.getAtIndex(ValueLayout.JAVA_INT, swapIndex);

            indexes.setAtIndex(ValueLayout.JAVA_INT, i - 1, secondValue);
            indexes.setAtIndex(ValueLayout.JAVA_INT, swapIndex, firstValue);
        }
    }


    private static MemorySegment initFile(Path path, int globalVertexCount, int verticesPerPage,
                                          int pageSize, Arena arena) throws IOException {
        var fileLength = calculateRequestedFileLength(globalVertexCount, pageSize, verticesPerPage);
        MemorySegment diskCache;
        try (var rwFile = new RandomAccessFile(path.toFile(), "rw")) {
            rwFile.setLength(fileLength);

            var channel = rwFile.getChannel();
            diskCache = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileLength, arena.scope());
        }

        return diskCache;
    }


    private static void checkRequestedFreeSpace(Path dbPath, int size, int totalPartitionsSize,
                                                int maxConnectionsPerVertex, int pageSize,
                                                int verticesPerPage) throws IOException {
        var fileStore = Files.getFileStore(dbPath);
        var usableSpace = fileStore.getUsableSpace();
        var requiredGraphSpace = calculateRequestedFileLength(size, pageSize, verticesPerPage);

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

    private static long calculateRequestedFileLength(long verticesCount, int pageSize, int verticesPerPage) {
        var pagesToWrite = pagesToWrite(verticesCount, verticesPerPage);
        return (long) pagesToWrite * pageSize;
    }

    private static int pagesToWrite(long verticesCount, int verticesPerPage) {
        return (int) (verticesCount + verticesPerPage - 1) / verticesPerPage;
    }

    private static long calculateGraphPartitionSize(long partitionSize, int maxConnectionsPerVertex, int vectorDim) {
        //1. edges
        //2. global indexes
        //3. vertex records
        return partitionSize * (maxConnectionsPerVertex + 1) * Integer.BYTES +
                partitionSize * Integer.BYTES + partitionSize * vectorDim * Float.BYTES;
    }

    private static final class MMapedGraph implements AutoCloseable {
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
        private final int maxConnectionsPerVertex;
        private final int vectorDimensions;
        private final DistanceFunction distanceFunction;
        private final int maxAmountOfCandidates;
        private final int pageSize;
        private final int vertexRecordSize;
        private final int diskRecordVectorsOffset;
        private final MemorySegment diskCache;


        private MMapedGraph(int capacity, int id, String name, Path path, int maxConnectionsPerVertex,
                            int vectorDimensions, DistanceFunction distanceFunction, int maxAmountOfCandidates,
                            int pageSize, int vertexRecordSize, int diskRecordVectorsOffset,
                            MemorySegment diskCache) throws IOException {
            this(capacity, false, id, name, path, maxConnectionsPerVertex, vectorDimensions,
                    distanceFunction, maxAmountOfCandidates, pageSize, vertexRecordSize, diskRecordVectorsOffset, diskCache);
        }

        private MMapedGraph(int capacity, boolean skipVectors, int id, String name, Path path,
                            int maxConnectionsPerVertex, int vectorDimensions,
                            DistanceFunction distanceFunction, int maxAmountOfCandidates, int pageSize,
                            int vertexRecordSize, int diskRecordVectorsOffset,
                            MemorySegment diskCache) throws IOException {
            this.edgeVersions = new AtomicLongArray(capacity);
            this.name = name;
            this.path = path;
            this.id = id;
            this.maxConnectionsPerVertex = maxConnectionsPerVertex;
            this.vectorDimensions = vectorDimensions;
            this.distanceFunction = distanceFunction;
            this.maxAmountOfCandidates = maxAmountOfCandidates;
            this.pageSize = pageSize;
            this.vertexRecordSize = vertexRecordSize;
            this.diskRecordVectorsOffset = diskRecordVectorsOffset;
            this.diskCache = diskCache;

            this.edgesArena = Arena.openShared();

            var edgesLayout = MemoryLayout.sequenceLayout((long) (this.maxConnectionsPerVertex + 1) * capacity,
                    ValueLayout.JAVA_INT);
            var globalIndexesLayout = MemoryLayout.sequenceLayout(capacity, ValueLayout.JAVA_INT);

            filesTs = System.nanoTime();


            var edgesPath = edgesPath(id, name, path, filesTs);
            try (var edgesChannel = FileChannel.open(edgesPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE,
                    StandardOpenOption.READ)) {
                this.edges = edgesChannel.map(FileChannel.MapMode.READ_WRITE, 0, edgesLayout.byteSize(),
                        edgesArena.scope());
            }

            var globalIndexesPath = globalIndexesPath(id, name, path, filesTs);

            try (var globalIndexesChannel = FileChannel.open(globalIndexesPath,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
                this.globalIndexes = globalIndexesChannel.map(FileChannel.MapMode.READ_WRITE, 0,
                        globalIndexesLayout.byteSize(),
                        edgesArena.scope());
            }

            if (!skipVectors) {
                this.vectorsArena = Arena.openShared();
                var vectorsLayout = MemoryLayout.sequenceLayout((long) capacity * this.vectorDimensions, ValueLayout.JAVA_FLOAT);
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

            var meanVector = new float[vectorDimensions];

            for (var i = 0; i < size; i++) {
                var vectorOffset = vectorOffset(i);
                for (var j = 0; j < vectorDimensions; j++) {
                    meanVector[j] += vectors.get(ValueLayout.JAVA_FLOAT, vectorOffset + (long) j * Float.BYTES);
                }
            }

            for (var j = 0; j < vectorDimensions; j++) {
                meanVector[j] = meanVector[j] / size;
            }

            var minDistance = Double.POSITIVE_INFINITY;
            var medoidIndex = -1;

            for (var i = 0; i < size; i++) {
                var currentDistance = distanceFunction.computeDistance(vectors, (long) i * vectorDimensions,
                        meanVector, 0, vectorDimensions);

                if (currentDistance < minDistance) {
                    minDistance = currentDistance;
                    medoidIndex = i;
                }
            }

            return medoidIndex;
        }


        private void addVector(int globalIndex, MemorySegment vector) {
            var index = (long) size * vectorDimensions;

            MemorySegment.copy(vector, 0, vectors,
                    index * Float.BYTES,
                    (long) vectorDimensions * Float.BYTES);
            globalIndexes.setAtIndex(ValueLayout.JAVA_INT, size, globalIndex);

            size++;
        }

        private void greedySearchPrune(
                int startVertexIndex,
                int vertexIndexToPrune, IntOpenHashSet visitedVertexIndices,
                BoundedGreedyVertexPriorityQueue nearestCandidates, float distanceMultiplication) {
            visitedVertexIndices.clear();
            nearestCandidates.clear();

            var checkedVertices = new Int2FloatOpenHashMap(2 * maxAmountOfCandidates, Hash.FAST_LOAD_FACTOR);

            var startVectorOffset = vectorOffset(startVertexIndex);
            var queryVectorOffset = vectorOffset(vertexIndexToPrune);
            var dim = vectorDimensions;

            nearestCandidates.add(startVertexIndex, distanceFunction.computeDistance(vectors, startVectorOffset,
                    vectors, queryVectorOffset, dim), false, false);

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

                            distanceFunction.computeDistance(vectors, queryVectorOffset, vectors, vectorOffset1,
                                    vectors, vectorOffset2, vectors, vectorOffset3, vectors, vectorOffset4,
                                    dim, result);

                            nearestCandidates.add(vertexIndexes[0], result[0], false, false);
                            nearestCandidates.add(vertexIndexes[1], result[1], false, false);
                            nearestCandidates.add(vertexIndexes[2], result[2], false, false);
                            nearestCandidates.add(vertexIndexes[3], result[3], false, false);

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

                        var currentDistance = distanceFunction.computeDistance(vectors, queryVectorOffset, vectors, vectorOffset,
                                dim);
                        nearestCandidates.add(vertexIndex, currentDistance, false, false);
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
            var dim = vectorDimensions;
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
                    var currentDistance = entry.getFloatValue();

                    if (Float.isNaN(currentDistance)) {
                        vectorsToCalculate.add(candidateIndex);
                        if (vectorsToCalculate.size() == 4) {
                            var vectorIndexes = vectorsToCalculate.elements();

                            var vectorOffset1 = vectorOffset(vectorIndexes[0]);
                            var vectorOffset2 = vectorOffset(vectorIndexes[1]);
                            var vectorOffset3 = vectorOffset(vectorIndexes[2]);
                            var vectorOffset4 = vectorOffset(vectorIndexes[3]);

                            distanceFunction.computeDistance(vectors, vectorOffset, vectors, vectorOffset1,
                                    vectors, vectorOffset2, vectors, vectorOffset3, vectors, vectorOffset4,
                                    dim, result);

                            cachedCandidates.add(new RobustPruneVertex(vectorIndexes[0], result[0]));
                            cachedCandidates.add(new RobustPruneVertex(vectorIndexes[1], result[1]));
                            cachedCandidates.add(new RobustPruneVertex(vectorIndexes[2], result[2]));
                            cachedCandidates.add(new RobustPruneVertex(vectorIndexes[3], result[3]));

                            vectorsToCalculate.clear();
                        }
                    } else {
                        var candidate = new RobustPruneVertex(candidateIndex, currentDistance);
                        cachedCandidates.add(candidate);
                    }
                }

                if (!vectorsToCalculate.isEmpty()) {
                    var size = vectorsToCalculate.size();
                    var vectorIndexes = vectorsToCalculate.elements();
                    for (int i = 0; i < size; i++) {
                        var vectorIndex = vectorIndexes[i];

                        var vectorOff = vectorOffset(vectorIndex);
                        var currentDistance = distanceFunction.computeDistance(vectors, vectorOffset, vectors, vectorOff, dim);
                        cachedCandidates.add(new RobustPruneVertex(vectorIndex, currentDistance));
                    }

                    vectorsToCalculate.clear();
                }

                var candidatesToCalculate = new ArrayList<RobustPruneVertex>(4);
                var removedCandidates = new ArrayList<RobustPruneVertex>(cachedCandidates.size());

                //book half of the space to merge edges between candidates.
                var maxConnectionsPerVertex = this.maxConnectionsPerVertex / 2;

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

                                distanceFunction.computeDistance(vectors, minIndex, vectors, vectorOffset1,
                                        vectors, vectorOffset2, vectors, vectorOffset3,
                                        vectors, vectorOffset4, dim, result);

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
                                var currentDistance =
                                        distanceFunction.computeDistance(vectors, minIndex, vectors,
                                                vectorOffset(candidate.index), dim);
                                if (currentDistance * currentMultiplication <= candidate.distance) {
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
            return (long) vertexIndex * vectorDimensions * Float.BYTES;
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
                    (long) vectorDimensions * Float.BYTES);
        }

        private void setVectorNotThreadSafe(int vertexIndex, float[] vector) {
            var vectorOffset = vectorOffset(vertexIndex);
            MemorySegment.copy(MemorySegment.ofArray(vector), 0L, vectors, vectorOffset,
                    (long) vectorDimensions * Float.BYTES);
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


        private void generateRandomEdges(ProgressTracker progressTracker) {
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

                    progressTracker.progress((i * 100.0) / size);
                }
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

        private void saveVectorsToDisk() {
            var verticesPerPage = pageSize / vertexRecordSize;
            var size = this.size;

            for (long i = 0, vectorsIndex = 0; i < size; i++) {
                var vertexGlobalIndex = globalIndexes.getAtIndex(ValueLayout.JAVA_INT, i);

                var localPageOffset = (long) vertexGlobalIndex % verticesPerPage;
                var pageOffset = ((long) vertexGlobalIndex / verticesPerPage) * pageSize;

                var recordOffset = localPageOffset * vertexRecordSize + Long.BYTES + pageOffset;

                for (long j = 0; j < vectorDimensions; j++, vectorsIndex++) {
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

            var vectorToAssign = new float[vectorDimensions];
            var tmpVectorToAssign = new float[vectorDimensions];

            for (int i = 0; i < size; i++) {
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
            Files.delete(globalIndexesPath);
        }
    }

    private static final class MmapVectorReader implements VectorReader {
        private final int recordSize;
        private final MemorySegment segment;

        private final Arena arena;

        private final int vectorDimensions;

        private final int size;

        public MmapVectorReader(final int vectorDimensions, Path path) throws IOException {
            this.vectorDimensions = vectorDimensions;
            this.recordSize = Float.BYTES * vectorDimensions;


            arena = Arena.openShared();

            try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
                segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(path), arena.scope());
                this.size = (int) (channel.size() / recordSize);
            }
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public MemorySegment read(int index) {
            return segment.asSlice((long) index * recordSize, (long) Float.BYTES * vectorDimensions);
        }

        @Override
        public void close() {
            arena.close();
        }
    }
}
