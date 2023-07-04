package jetbrains.exodus.diskann;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import jetbrains.exodus.diskann.collections.BoundedGreedyVertexPriorityQueue;
import jetbrains.exodus.diskann.collections.NonBlockingHashMapLongLong;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.rng.sampling.PermutationSampler;
import org.apache.commons.rng.simple.RandomSource;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLongArray;

public final class DiskANN implements AutoCloseable {
    private static final int CORES = Runtime.getRuntime().availableProcessors();
    public static final byte L2_DISTANCE = 0;
    public static final byte DOT_DISTANCE = 1;

    private static final VectorSpecies<Float> species = FloatVector.SPECIES_PREFERRED;

    private static final int PAGE_SIZE_MULTIPLIER = 4 * 1024;

    private static final Logger logger = LoggerFactory.getLogger(DiskANN.class);

    private final int vectorDim;

    private final float distanceMultiplication;

    private final int maxConnectionsPerVertex;

    private final int maxAmountOfCandidates;

    private final int mutatorsQueueSize;
    private final int pqSubVectorSize;
    private final int pqQuantizersCount;

    private long verticesSize = 0;
    private final NonBlockingHashMapLongLong graphPages = new NonBlockingHashMapLongLong(1024, false);
    private final Arena diskCacheArena = Arena.openShared();
    private MemorySegment diskCache;

    private final ArrayList<ExecutorService> vectorMutationThreads = new ArrayList<>();

    /**
     * Size of vertex record in bytes.
     * <p>
     * 1. Vector data (4 bytes * vectorDim)
     * 2. Vector id (4 bytes)
     * 3. Real amount of edges (1 byte)
     * 4. Edges to other vertices (4 bytes * maxConnectionsPerVertex)
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
    private final long diskCacheRecordVectorsOffset;
    private final long diskCacheRecordIdOffset;
    private final long diskCacheRecordEdgesCountOffset;
    private final long diskCacheRecordEdgesOffset;

    private final long diskCacheRecordByteAlignment;

    private long visitedVerticesSum = 0;
    private long visitedVerticesCount = 0;

    private long testedVerticesSum = 0;
    private long testedVerticesCount = 0;

    private double pqDistanceError = 0;
    private int pqDistancesConverted = 0;

    //1st dimension quantizer index
    //2nd index of code inside code book
    //3d dimension centroid vector
    private float[][][] pqCentroids;
    private byte[] pqVectors;

    private final ThreadLocal<NearestGreedySearchCachedData> nearestGreedySearchCachedDataThreadLocal;

    public DiskANN(String name, int vectorDim, byte distanceFunction) {
        this(name, vectorDim, distanceFunction, 2.1f,
                64, 128, 1024,
                16);
    }

    public DiskANN(String name, int vectorDim, byte distanceFunction,
                   float distanceMultiplication,
                   int maxConnectionsPerVertex,
                   int maxAmountOfCandidates,
                   int mutatorsQueueSize,
                   int pqCompression) {
        this.vectorDim = vectorDim;
        this.distanceMultiplication = distanceMultiplication;
        this.maxConnectionsPerVertex = maxConnectionsPerVertex;
        this.maxAmountOfCandidates = maxAmountOfCandidates;
        this.mutatorsQueueSize = mutatorsQueueSize;
        this.distanceFunction = distanceFunction;

        MemoryLayout diskCacheRecordLayout = MemoryLayout.structLayout(
                MemoryLayout.sequenceLayout(vectorDim, ValueLayout.JAVA_FLOAT).withName("vector"),
                ValueLayout.JAVA_LONG.withName("id"),
                MemoryLayout.sequenceLayout(maxConnectionsPerVertex, ValueLayout.JAVA_LONG).withName("edges"),
                ValueLayout.JAVA_BYTE.withName("edgesCount")
        );

        diskCacheRecordByteAlignment = diskCacheRecordLayout.byteAlignment();
        this.vertexRecordSize = (int) (
                ((diskCacheRecordLayout.byteSize() + diskCacheRecordByteAlignment - 1)
                        / diskCacheRecordLayout.byteAlignment()) * diskCacheRecordByteAlignment
        );

        diskCacheRecordVectorsOffset = diskCacheRecordLayout.byteOffset(
                MemoryLayout.PathElement.groupElement("vector"));
        diskCacheRecordIdOffset = diskCacheRecordLayout.byteOffset(
                MemoryLayout.PathElement.groupElement("id"));
        diskCacheRecordEdgesCountOffset = diskCacheRecordLayout.byteOffset(
                MemoryLayout.PathElement.groupElement("edgesCount"));
        diskCacheRecordEdgesOffset = diskCacheRecordLayout.byteOffset(
                MemoryLayout.PathElement.groupElement("edges"));


        if (this.vertexRecordSize > PAGE_SIZE_MULTIPLIER - 1) {
            this.pageSize = ((vertexRecordSize + PAGE_SIZE_MULTIPLIER - 1 - Long.BYTES) /
                    (PAGE_SIZE_MULTIPLIER - Long.BYTES)) * PAGE_SIZE_MULTIPLIER;
        } else {
            this.pageSize = PAGE_SIZE_MULTIPLIER;
        }


        this.verticesPerPage = (pageSize - Long.BYTES) / vertexRecordSize;


        if (logger.isInfoEnabled()) {
            logger.info("Vector index " + name + " has been initialized. Vector lane count for distance calculation " +
                    "is " + species.length());
        }

        if (logger.isInfoEnabled()) {
            logger.info("Using " + CORES + " cores for processing of vectors");
        }

        for (var i = 0; i < CORES; i++) {
            var id = i;
            vectorMutationThreads.add(Executors.newSingleThreadExecutor(r -> {
                var thread = new Thread(r, name + "- vector mutator-" + id);
                thread.setDaemon(true);
                return thread;
            }));
        }

        pqSubVectorSize = pqCompression / Float.BYTES;
        pqQuantizersCount = this.vectorDim / pqSubVectorSize;

        if (pqCompression % Float.BYTES != 0) {
            throw new IllegalArgumentException(
                    "Vector should be divided during creation of PQ codes without remainder.");
        }

        if (vectorDim % pqSubVectorSize != 0) {
            throw new IllegalArgumentException(
                    "Vector should be divided during creation of PQ codes without remainder.");
        }

        nearestGreedySearchCachedDataThreadLocal = ThreadLocal.withInitial(() -> new NearestGreedySearchCachedData(
                new IntOpenHashSet(8 * 1024,
                        Hash.VERY_FAST_LOAD_FACTOR), new float[pqQuantizersCount * (1 << Byte.SIZE)],
                new BoundedGreedyVertexPriorityQueue(maxAmountOfCandidates)));
    }


    public void buildIndex(VectorReader vectorReader) {
        logger.info("Generating PQ codes for vectors...");
        generatePQCodes(vectorReader);
        logger.info("PQ codes for vectors have been generated.");

        var size = vectorReader.size();
        try (var graph = new InMemoryGraph(size)) {
            for (var i = 0; i < size; i++) {
                var vertex = vectorReader.read(i);
                graph.addVector(vertex.leftLong(), vertex.right());
            }

            graph.generateRandomEdges();
            var medoid = graph.medoid();

            pruneIndex(size, graph, medoid, distanceMultiplication);
            graph.saveToDisk();

            diskGraph = new DiskGraph(medoid);
            verticesSize = size;
        }
    }

    public long[] nearest(float[] vector, int resultSize) {
        var nearestVertices = diskGraph.greedySearchNearest(vector,
                resultSize);
        var ids = new long[nearestVertices.length];

        for (var index = 0; index < nearestVertices.length; index++) {
            ids[index] = diskGraph.fetchId(nearestVertices[index]);
        }

        return ids;
    }

    public void resetVisitStats() {
        visitedVerticesSum = 0;
        visitedVerticesCount = 0;
    }

    public void resetTestStats() {
        testedVerticesSum = 0;
        testedVerticesCount = 0;
    }

    public void resetPQDistanceStats() {
        pqDistanceError = 0;
        pqDistancesConverted = 0;
    }

    public double getPQDistanceError() {
        return pqDistanceError / pqDistancesConverted;
    }

    public long getVisitedVerticesAvg() {
        return visitedVerticesSum / visitedVerticesCount;
    }

    public long getTestedVerticesAvg() {
        return testedVerticesSum / testedVerticesCount;
    }


    private void pruneIndex(long size, InMemoryGraph graph, long medoid, float distanceMultiplication) {
        var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
        var permutation = new PermutationSampler(rng, (int) size, (int) size).sample();

        if (logger.isInfoEnabled()) {
            logger.info("Graph pruning started with distance multiplication " + distanceMultiplication + ".");
        }

        var mutatorFutures = new ArrayList<Future<ArrayList<Future<?>>>>();
        for (int vertexIndex : permutation) {
            var mutatorIndex = (vertexIndex % vectorMutationThreads.size());
            var mutator = vectorMutationThreads.get(mutatorIndex);

            var mutatorFuture = mutator.submit(() -> {
                graph.greedySearchPrune(medoid, vertexIndex);

                var features = new ArrayList<Future<?>>(maxConnectionsPerVertex + 1);
                var neighbours = graph.fetchNeighbours(vertexIndex);
                for (var neighbour : neighbours) {
                    var neighbourNeighbours = graph.fetchNeighbours(neighbour);
                    if (!ArrayUtils.contains(neighbourNeighbours, vertexIndex)) {
                        var neighbourMutatorIndex = (int) (neighbour % vectorMutationThreads.size());
                        var neighbourMutator = vectorMutationThreads.get(neighbourMutatorIndex);
                        var neighborMutatorFuture = neighbourMutator.submit(() -> {
                            if (graph.getNeighboursSize(neighbour) + 1 <= maxConnectionsPerVertex) {
                                graph.acquireVertex(neighbour);
                                try {
                                    graph.appendNeighbour(neighbour, vertexIndex);
                                } finally {
                                    graph.releaseVertex(neighbour);
                                }
                            } else {
                                graph.robustPrune(
                                        neighbour,
                                        new LongOpenHashSet(new long[]{vertexIndex}),
                                        distanceMultiplication
                                );
                            }
                        });
                        features.add(neighborMutatorFuture);
                    }
                }

                return features;
            });

            mutatorFutures.add(mutatorFuture);

            if (mutatorFutures.size() == mutatorsQueueSize) {
                for (var feature : mutatorFutures) {
                    ArrayList<Future<?>> subFutures;
                    try {
                        subFutures = feature.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new IllegalStateException(e);
                    }
                    for (var subFuture : subFutures) {
                        try {
                            subFuture.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new IllegalStateException(e);
                        }
                    }
                }

                mutatorFutures.clear();
            }
        }

        for (var feature : mutatorFutures) {
            ArrayList<Future<?>> subFutures;
            try {
                subFutures = feature.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
            for (var subFuture : subFutures) {
                try {
                    subFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info("Graph pruning: " + size + " vertices were processed.");
        }
    }

    private void generatePQCodes(VectorReader vectorReader) {
        pqCentroids = new float[pqQuantizersCount][][];
        var kMeans = new KMeansMiniBatchSGD[pqQuantizersCount];

        for (int i = 0; i < pqQuantizersCount; i++) {
            kMeans[i] = new KMeansMiniBatchSGD(1 << Byte.SIZE, i * pqSubVectorSize,
                    pqSubVectorSize, vectorReader);
        }

        var finishedCount = 0;

        while (finishedCount < pqQuantizersCount) {
            for (var km : kMeans) {
                var iteration = km.nextIteration(16, distanceFunction);
                if (iteration == 1_000) {
                    finishedCount++;
                }
            }
        }

        for (int i = 0; i < pqQuantizersCount; i++) {
            pqCentroids[i] = kMeans[i].centroids;
        }

        var size = vectorReader.size();
        pqVectors = new byte[size * pqQuantizersCount];

        for (int n = 0; n < size; n++) {
            var vector = vectorReader.read(n).right();

            for (int i = 0; i < pqQuantizersCount; i++) {
                var centroidIndex = findClosestCentroid(distanceFunction, kMeans[i].centroids, vector, i * pqSubVectorSize);
                pqVectors[n * pqQuantizersCount + i] = (byte) centroidIndex;
            }
        }
    }

    private float[] buildPQDistanceLookupTable(float[] vector) {
        var lookupTable = this.nearestGreedySearchCachedDataThreadLocal.get().lookupTable;

        for (int i = 0; i < pqQuantizersCount; i++) {
            var centroids = pqCentroids[i];

            for (int j = 0; j < centroids.length; j++) {
                var distance = computeDistance(distanceFunction, centroids[j], vector,
                        i * pqSubVectorSize);
                lookupTable[i * (1 << Byte.SIZE) + j] = distance;
            }
        }

        return lookupTable;
    }

    private float computePQDistance(float[] lookupTable, int vectorIndex) {
        var distance = 0f;

        var pqIndex = pqQuantizersCount * vectorIndex;
        for (int i = pqIndex; i < pqIndex + pqQuantizersCount; i++) {
            var code = pqVectors[i];
            distance += lookupTable[(i - pqIndex) * (1 << Byte.SIZE) + (code & 0xFF)];
        }

        return distance;
    }

    static int findClosestCentroid(final byte distanceFunction, float[][] centroids, float[] vector, int from) {
        var minDistance = Float.MAX_VALUE;
        var minIndex = -1;

        for (int i = 0; i < centroids.length; i++) {
            var distance = DiskANN.computeDistance(distanceFunction, centroids[i], vector, from);

            if (distance < minDistance) {
                minDistance = distance;
                minIndex = i;
            }
        }

        return minIndex;
    }

    private float computeDistance(MemorySegment firstSegment, long firstSegmentFromOffset, MemorySegment secondSegment,
                                  long secondSegmentFromOffset, int size) {
        if (distanceFunction == L2_DISTANCE) {
            return computeL2Distance(firstSegment, firstSegmentFromOffset, secondSegment, secondSegmentFromOffset,
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
            return computeL2Distance(firstSegment, firstSegmentFromOffset, secondVector);
        } else if (distanceFunction == DOT_DISTANCE) {
            return computeDotDistance(firstSegment, firstSegmentFromOffset, secondVector);
        } else {
            throw new IllegalStateException("Unknown distance function: " + distanceFunction);
        }
    }

    static float[] computeGradientStep(float[] centroid, float[] point, int pointOffset, float learningRate) {
        var result = new float[centroid.length];
        var index = 0;
        var learningRateVector = FloatVector.broadcast(species, learningRate);
        while (index < species.loopBound(centroid.length)) {
            var centroidVector = FloatVector.fromArray(species, centroid, index);
            var pointVector = FloatVector.fromArray(species, point, index + pointOffset);

            var diff = pointVector.sub(centroidVector);
            centroidVector = diff.fma(learningRateVector, centroidVector);
            centroidVector.intoArray(result, index);
            index += species.length();
        }

        while (index < centroid.length) {
            result[index] = centroid[index] + learningRate * (point[index + pointOffset] - centroid[index]);
            index++;
        }

        return result;
    }


    static float computeDistance(final byte distanceFunction, float[] firstVector, float[] secondVector, int secondVectorFrom) {
        if (distanceFunction == L2_DISTANCE) {
            return computeL2Distance(firstVector, secondVector, secondVectorFrom);
        } else if (distanceFunction == DOT_DISTANCE) {
            return computeDotDistance(firstVector, secondVector, secondVectorFrom);
        } else {
            throw new IllegalStateException("Unknown distance function: " + distanceFunction);
        }
    }

    static float computeL2Distance(float[] firstVector, float[] secondVector, int secondVectorFrom) {
        var sumVector = FloatVector.zero(species);
        var index = 0;

        while (index < species.loopBound(firstVector.length)) {
            var first = FloatVector.fromArray(species, firstVector, index);
            var second = FloatVector.fromArray(species, secondVector, index + secondVectorFrom);

            var diff = first.sub(second);
            sumVector = diff.fma(diff, sumVector);
            index += species.length();
        }

        var sum = sumVector.reduceLanes(VectorOperators.ADD);

        while (index < firstVector.length) {
            var diff = firstVector[index] - secondVector[index + secondVectorFrom];
            sum += diff * diff;
            index++;
        }

        return sum;
    }

    static float computeL2Distance(MemorySegment firstSegment, long firstSegmentFromOffset, float[] secondVector) {
        var sumVector = FloatVector.zero(species);
        var index = 0;

        while (index < species.loopBound(secondVector.length)) {
            var first = FloatVector.fromMemorySegment(species, firstSegment,
                    firstSegmentFromOffset + (long) index * Float.BYTES, ByteOrder.nativeOrder());
            var second = FloatVector.fromArray(species, secondVector, index);

            var diff = first.sub(second);
            sumVector = diff.fma(diff, sumVector);
            index += species.length();
        }

        var sum = sumVector.reduceLanes(VectorOperators.ADD);

        while (index < secondVector.length) {
            var diff = firstSegment.get(ValueLayout.JAVA_FLOAT,
                    firstSegmentFromOffset + (long) index * Float.BYTES)
                    - secondVector[index];
            sum += diff * diff;
            index++;
        }

        return sum;
    }

    static float computeDotDistance(float[] firstVector, float[] secondVector, int secondVectorFrom) {
        var sumVector = FloatVector.zero(species);
        var index = 0;

        while (index < species.loopBound(firstVector.length)) {
            var first = FloatVector.fromArray(species, firstVector, index);
            var second = FloatVector.fromArray(species, secondVector, index + secondVectorFrom);

            sumVector = first.fma(second, sumVector);
            index += species.length();
        }

        var sum = sumVector.reduceLanes(VectorOperators.ADD);

        while (index < firstVector.length) {
            var mul = firstVector[index] * secondVector[index + secondVectorFrom];
            sum += mul;
            index++;
        }

        return -sum;
    }

    static float computeDotDistance(MemorySegment firstSegment, long firstSegmentFromOffset, float[] secondVector) {
        var sumVector = FloatVector.zero(species);
        var index = 0;

        while (index < species.loopBound(secondVector.length)) {
            var first = FloatVector.fromMemorySegment(species, firstSegment,
                    firstSegmentFromOffset + (long) index * Float.BYTES, ByteOrder.nativeOrder());
            var second = FloatVector.fromArray(species, secondVector, index);

            sumVector = first.fma(second, sumVector);
            index += species.length();
        }

        var sum = sumVector.reduceLanes(VectorOperators.ADD);

        while (index < secondVector.length) {
            var mul = firstSegment.get(ValueLayout.JAVA_FLOAT, firstSegmentFromOffset + (long) index * Float.BYTES)
                    * secondVector[index];
            sum += mul;
            index++;
        }

        return -sum;
    }

    static float computeL2Distance(MemorySegment firstSegment, long firstSegmentFromOffset,
                                   MemorySegment secondSegment,
                                   long secondSegmentFromOffset, int size) {

        var sumVector = FloatVector.zero(species);
        var index = 0;

        while (index < species.loopBound(size)) {
            var first = FloatVector.fromMemorySegment(species, firstSegment,
                    firstSegmentFromOffset + (long) index * Float.BYTES, ByteOrder.nativeOrder());
            var second = FloatVector.fromMemorySegment(species, secondSegment,
                    secondSegmentFromOffset + (long) index * Float.BYTES, ByteOrder.nativeOrder());

            var diff = first.sub(second);
            sumVector = diff.fma(diff, sumVector);
            index += species.length();
        }

        var sum = sumVector.reduceLanes(VectorOperators.ADD);

        while (index < size) {
            var diff = firstSegment.get(ValueLayout.JAVA_FLOAT,
                    firstSegmentFromOffset + (long) index * Float.BYTES)
                    - secondSegment.get(ValueLayout.JAVA_FLOAT,
                    secondSegmentFromOffset + (long) index * Float.BYTES);
            sum += diff * diff;
            index++;
        }

        return sum;
    }

    static float computeDotDistance(MemorySegment firstSegment, long firstSegmentFromOffset,
                                    MemorySegment secondSegment,
                                    long secondSegmentFromOffset, int size) {

        var sumVector = FloatVector.zero(species);
        var index = 0;

        while (index < species.loopBound(size)) {
            var first = FloatVector.fromMemorySegment(species, firstSegment,
                    firstSegmentFromOffset + (long) index * Float.BYTES,
                    ByteOrder.nativeOrder());
            var second = FloatVector.fromMemorySegment(species, secondSegment,
                    secondSegmentFromOffset + (long) index * Float.BYTES, ByteOrder.nativeOrder());

            sumVector = first.fma(second, sumVector);
            index += species.length();
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


    @Override
    public void close() {
        diskCacheArena.close();
    }

    private final class InMemoryGraph implements AutoCloseable {
        private int size = 0;

        private final MemorySegment struct;
        private final long vectorsOffset;
        private final long idsOffset;
        private final long edgesOffset;

        private final AtomicLongArray edgeVersions;

        private long medoid = -1;

        private final Arena inMemoryGraphArea;

        private InMemoryGraph(int capacity) {
            this.edgeVersions = new AtomicLongArray(capacity);
            this.inMemoryGraphArea = Arena.openShared();

            var layout = MemoryLayout.structLayout(
                    MemoryLayout.sequenceLayout((long) capacity * vectorDim, ValueLayout.JAVA_FLOAT).withName("vectors"),
                    MemoryLayout.sequenceLayout(capacity, ValueLayout.JAVA_LONG).withName("ids"),
                    MemoryLayout.sequenceLayout((long) (maxConnectionsPerVertex + 1) * capacity,
                            ValueLayout.JAVA_LONG).withName("edges")
            );
            this.struct = inMemoryGraphArea.allocate(layout);

            this.vectorsOffset = layout.byteOffset(MemoryLayout.PathElement.groupElement("vectors"));
            this.idsOffset = layout.byteOffset(MemoryLayout.PathElement.groupElement("ids"));
            this.edgesOffset = layout.byteOffset(MemoryLayout.PathElement.groupElement("edges"));
        }


        private void addVector(long id, float[] vector) {
            var index = size * vectorDim;


            MemorySegment.copy(vector, 0, struct, ValueLayout.JAVA_FLOAT,
                    vectorsOffset + (long) index * Float.BYTES,
                    vectorDim);

            struct.set(ValueLayout.JAVA_LONG, idsOffset + (long) size * Long.BYTES, id);
            size++;

            medoid = -1;
        }

        private void greedySearchPrune(
                long startVertexIndex,
                long vertexIndexToPrune) {
            var nearestCandidates = new TreeSet<GreedyVertex>();
            var processingQueue = new PriorityQueue<GreedyVertex>();

            var visitedVertexIndices = new LongOpenHashSet(2 * maxAmountOfCandidates, Hash.FAST_LOAD_FACTOR);


            var startVectorOffset = vectorOffset(startVertexIndex);
            var queryVectorOffset = vectorOffset(vertexIndexToPrune);
            var dim = vectorDim;

            processingQueue.add(new GreedyVertex(startVertexIndex, computeDistance(struct, startVectorOffset,
                    struct, queryVectorOffset, dim), false));

            while (!processingQueue.isEmpty()) {
                assert nearestCandidates.size() <= maxAmountOfCandidates;
                var currentVertex = processingQueue.poll();

                if (nearestCandidates.size() == maxAmountOfCandidates &&
                        nearestCandidates.last().distance < currentVertex.distance) {
                    break;
                } else {
                    if (nearestCandidates.size() == maxAmountOfCandidates) {
                        nearestCandidates.pollLast();
                    }
                    nearestCandidates.add(currentVertex);
                }

                var vertexNeighbours = fetchNeighbours(currentVertex.index);
                for (var vertexIndex : vertexNeighbours) {
                    if (visitedVertexIndices.add(vertexIndex)) {
                        //return array and offset instead
                        var distance = computeDistance(struct, queryVectorOffset, struct, vectorOffset(vertexIndex),
                                dim);
                        processingQueue.add(new GreedyVertex(vertexIndex, distance, false));
                    }
                }
            }

            assert nearestCandidates.size() <= maxAmountOfCandidates;
            robustPrune(vertexIndexToPrune, visitedVertexIndices, distanceMultiplication);
        }

        private void robustPrune(
                long vertexIndex,
                LongOpenHashSet neighboursCandidates,
                float distanceMultiplication
        ) {
            var dim = vectorDim;
            acquireVertex(vertexIndex);
            try {
                LongOpenHashSet candidates;
                if (getNeighboursSize(vertexIndex) > 0) {
                    var newCandidates = neighboursCandidates.clone();
                    newCandidates.addAll(LongArrayList.wrap(getNeighboursAndClear(vertexIndex)));

                    candidates = newCandidates;
                } else {
                    candidates = neighboursCandidates;
                }

                candidates.remove(vertexIndex);

                var vectorOffset = vectorOffset(vertexIndex);
                var candidatesIterator = candidates.longIterator();

                var cachedCandidates = new TreeSet<RobustPruneVertex>();
                while (candidatesIterator.hasNext()) {
                    var candidateIndex = candidatesIterator.nextLong();

                    var distance = computeDistance(struct, vectorOffset, struct,
                            vectorOffset(candidateIndex), dim);
                    var candidate = new RobustPruneVertex(candidateIndex, distance);

                    cachedCandidates.add(candidate);
                }


                var neighbours = new LongArrayList(maxConnectionsPerVertex);
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
                        var iterator = cachedCandidates.iterator();
                        while (iterator.hasNext()) {
                            var candidate = iterator.next();
                            var distance = computeDistance(struct, minIndex, struct, vectorOffset(candidate.index),
                                    dim);

                            if (distance * currentMultiplication <= candidate.distance) {
                                iterator.remove();

                                if (distance * distanceMultiplication > candidate.distance) {
                                    removed.add(candidate);
                                }
                            }
                        }
                    }

                    currentMultiplication *= 1.2;
                }

                setNeighbours(vertexIndex, neighbours.elements(), neighbours.size());
            } finally {
                releaseVertex(vertexIndex);
            }
        }

        private long vectorOffset(long vertexIndex) {
            return vertexIndex * vectorDim * Float.BYTES + vectorsOffset;
        }


        private int getNeighboursSize(long vertexIndex) {
            var version = edgeVersions.get((int) vertexIndex);
            while (true) {
                var size = (int) struct.get(ValueLayout.JAVA_LONG,
                        vertexIndex * (maxConnectionsPerVertex + 1) * Long.BYTES + edgesOffset);
                var newVersion = edgeVersions.get((int) vertexIndex);

                VarHandle.acquireFence();
                if (newVersion == version) {
                    assert size >= 0 && size <= maxConnectionsPerVertex;
                    return size;
                }

                version = newVersion;
            }
        }


        @NotNull
        private long[] fetchNeighbours(long vertexIndex) {
            var version = edgeVersions.get((int) vertexIndex);

            while (true) {
                var edgesIndex = vertexIndex * (maxConnectionsPerVertex + 1);
                var size = (int) struct.get(ValueLayout.JAVA_LONG,
                        edgesIndex * Long.BYTES + edgesOffset);

                var result = new long[size];
                MemorySegment.copy(struct, edgesIndex * Long.BYTES + edgesOffset + Long.BYTES,
                        MemorySegment.ofArray(result), 0L, (long) size * Long.BYTES);
                var newVersion = edgeVersions.get((int) vertexIndex);

                VarHandle.acquireFence();
                if (newVersion == version) {
                    assert size <= maxConnectionsPerVertex;
                    return result;
                }

                version = newVersion;
            }
        }

        private void setNeighbours(long vertexIndex, long[] neighbours, int size) {
            validateLocked(vertexIndex);
            assert (size >= 0 && size <= maxConnectionsPerVertex);

            var edgesOffset = (vertexIndex * (maxConnectionsPerVertex + 1)) * Long.BYTES + this.edgesOffset;
            struct.set(ValueLayout.JAVA_LONG, edgesOffset, size);

            MemorySegment.copy(MemorySegment.ofArray(neighbours), 0L, struct, edgesOffset + Long.BYTES,
                    (long) size * Long.BYTES);
        }

        private void appendNeighbour(long vertexIndex, long neighbour) {
            validateLocked(vertexIndex);

            var edgesOffset = (vertexIndex * (maxConnectionsPerVertex + 1)) * Long.BYTES + this.edgesOffset;
            var size = (int) struct.get(ValueLayout.JAVA_LONG, edgesOffset);

            assert size + 1 <= maxConnectionsPerVertex;

            struct.set(ValueLayout.JAVA_LONG, edgesOffset, size + 1);
            struct.set(ValueLayout.JAVA_LONG, edgesOffset + (long) (size + 1) * Long.BYTES, neighbour);
        }


        private void generateRandomEdges() {
            if (size == 1) {
                return;
            }

            var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
            var shuffledIndexes = PermutationSampler.natural(size);
            PermutationSampler.shuffle(rng, shuffledIndexes);

            var maxEdges = Math.min(size - 1, maxConnectionsPerVertex);
            var shuffleIndex = 0;
            for (var i = 0; i < size; i++) {
                var edgesOffset = (long) i * (maxConnectionsPerVertex + 1) * Long.BYTES + this.edgesOffset;
                struct.set(ValueLayout.JAVA_LONG, edgesOffset, maxEdges);

                var addedEdges = 0;
                while (addedEdges < maxEdges) {
                    var randomIndex = shuffledIndexes[shuffleIndex];
                    shuffleIndex++;

                    if (shuffleIndex == size) {
                        PermutationSampler.shuffle(rng, shuffledIndexes);
                        shuffleIndex = 0;
                    } else if (randomIndex == i) {
                        continue;
                    }

                    struct.set(ValueLayout.JAVA_LONG, edgesOffset + Long.BYTES, randomIndex);
                    edgesOffset += Long.BYTES;
                    addedEdges++;
                }
            }
        }

        private long[] getNeighboursAndClear(long vertexIndex) {
            validateLocked(vertexIndex);
            var edgesOffset = (vertexIndex * (maxConnectionsPerVertex + 1)) * Long.BYTES + this.edgesOffset;
            var result = fetchNeighbours(vertexIndex);

            edgeVersions.incrementAndGet((int) vertexIndex);
            struct.set(ValueLayout.JAVA_LONG, edgesOffset, 0L);
            edgeVersions.incrementAndGet((int) vertexIndex);

            return result;
        }

        private long medoid() {
            if (medoid == -1L) {
                medoid = calculateMedoid();
            }

            return medoid;
        }

        private void acquireVertex(long vertexIndex) {
            while (true) {
                var version = edgeVersions.get((int) vertexIndex);
                if ((version & 1L) != 0L) {
                    throw new IllegalStateException("Vertex $vertexIndex is already acquired");
                }
                if (edgeVersions.compareAndSet((int) vertexIndex, version, version + 1)) {
                    return;
                }
            }
        }

        private void validateLocked(long vertexIndex) {
            var version = edgeVersions.get((int) vertexIndex);
            if ((version & 1L) != 1L) {
                throw new IllegalStateException("Vertex $vertexIndex is not acquired");
            }
        }

        private void releaseVertex(long vertexIndex) {
            while (true) {
                var version = edgeVersions.get((int) vertexIndex);
                if ((version & 1L) != 1L) {
                    throw new IllegalStateException("Vertex $vertexIndex is not acquired");
                }
                if (edgeVersions.compareAndSet((int) vertexIndex, version, version + 1)) {
                    return;
                }
            }
        }

        private long calculateMedoid() {
            if (size == 1) {
                return 0;
            }

            var meanVector = new float[vectorDim];

            for (var i = 0; i < size; i++) {
                var vectorOffset = vectorOffset(i);
                for (var j = 0; j < vectorDim; j++) {
                    meanVector[j] += struct.get(ValueLayout.JAVA_FLOAT, vectorOffset + (long) j * Float.BYTES);
                }
            }

            for (var j = 0; j < vectorDim; j++) {
                meanVector[j] = meanVector[j] / size;
            }

            var minDistance = Double.POSITIVE_INFINITY;
            var medoidIndex = -1;

            for (var i = 0; i < size; i++) {
                var distance = computeDistance(struct, (long) i * vectorDim, meanVector);

                if (distance < minDistance) {
                    minDistance = distance;
                    medoidIndex = i;
                }
            }

            return medoidIndex;
        }


        private void saveToDisk() {
            var pagesToWrite = (size + verticesPerPage - 1 / verticesPerPage);
            diskCache = diskCacheArena.allocate((long) pagesToWrite * pageSize,
                    diskCacheRecordByteAlignment);

            var verticesPerPage = pageSize / vertexRecordSize;

            var wittenVertices = 0;

            var vectorsIndex = 0;
            var pageSegmentOffset = 0;

            while (wittenVertices < size) {
                diskCache.set(ValueLayout.JAVA_LONG, pageSegmentOffset, size);

                var verticesToWrite = Math.min(verticesPerPage, size - wittenVertices);
                for (var i = 0; i < verticesToWrite; i++) {
                    var recordOffset = (long) i * vertexRecordSize + Long.BYTES + pageSegmentOffset;

                    var edgesIndex = wittenVertices * (maxConnectionsPerVertex + 1);

                    for (var j = 0; j < vectorDim; j++) {
                        var vectorItem = struct.get(ValueLayout.JAVA_FLOAT, vectorsOffset +
                                (long) vectorsIndex * Float.BYTES);
                        diskCache.set(ValueLayout.JAVA_FLOAT,
                                recordOffset + diskCacheRecordVectorsOffset +
                                        (long) j * Float.BYTES, vectorItem);
                        vectorsIndex++;
                    }

                    var vectorId = struct.get(ValueLayout.JAVA_LONG,
                            idsOffset + (long) wittenVertices * Long.BYTES);
                    diskCache.set(ValueLayout.JAVA_LONG,
                            recordOffset + diskCacheRecordIdOffset, vectorId);

                    var edgesSize = struct.get(ValueLayout.JAVA_LONG,
                            (long) edgesIndex * Long.BYTES + edgesOffset);
                    assert (edgesSize >= 0 && edgesSize <= maxConnectionsPerVertex);
                    edgesIndex++;

                    for (var j = 0; j < edgesSize; j++) {
                        var edgesOffset = (long) edgesIndex * Long.BYTES + this.edgesOffset;
                        var neighbourIndex = struct.get(ValueLayout.JAVA_LONG, edgesOffset);
                        diskCache.set(ValueLayout.JAVA_LONG,
                                recordOffset + diskCacheRecordEdgesOffset + (long) j * Long.BYTES, neighbourIndex);
                        edgesIndex++;
                    }
                    diskCache.set(ValueLayout.JAVA_BYTE,
                            recordOffset + diskCacheRecordEdgesCountOffset, (byte) edgesSize);
                    wittenVertices++;
                }

                graphPages.put(pageSegmentOffset / pageSize, pageSegmentOffset);
                pageSegmentOffset += pageSize;
            }
        }

        @Override
        public void close() {
            inMemoryGraphArea.close();
        }
    }

    private final class DiskGraph {
        private final long medoid;

        private DiskGraph(long medoid) {
            this.medoid = medoid;
        }

        private int[] greedySearchNearest(
                float[] queryVector,
                int maxResultSize
        ) {
            var threadLocalCache = nearestGreedySearchCachedDataThreadLocal.get();

            var visitedVertexIndices = threadLocalCache.visistedVertexIndices;
            visitedVertexIndices.clear();

            var nearestCandidates = threadLocalCache.nearestCandidates;
            nearestCandidates.clear();

            var startVertexIndex = medoid;
            var startVectorOffset = vectorOffset(startVertexIndex);


            nearestCandidates.add((int) startVertexIndex, computeDistance(diskCache, startVectorOffset, queryVector), false);

            assert nearestCandidates.size() <= maxAmountOfCandidates;
            visitedVertexIndices.add((int) startVertexIndex);
            testedVerticesSum++;

            float[] lookupTable = null;
            var visited = 0;

            while (true) {
                int currentVertex = -1;
                while (true) {
                    var notCheckedVertex = nearestCandidates.nextNotCheckedVertexIndex();
                    if (notCheckedVertex < 0) {
                        break;
                    }
                    if (nearestCandidates.isPqDistance(notCheckedVertex)) {
                        var vertexIndex = nearestCandidates.vertexIndex(notCheckedVertex);
                        var pqDistance = nearestCandidates.vertexDistance(notCheckedVertex);

                        var preciseDistance = computeDistance(diskCache, vectorOffset(vertexIndex),
                                queryVector);

                        if (preciseDistance != 0) {
                            pqDistanceError += 100 * Math.abs(pqDistance - preciseDistance) / preciseDistance;
                            pqDistancesConverted++;
                        }

                        nearestCandidates.resortVertex(notCheckedVertex, preciseDistance);
                    } else {
                        currentVertex = nearestCandidates.vertexIndex(notCheckedVertex);
                        break;
                    }
                }

                if (currentVertex < 0) {
                    break;
                }

                visited++;

                var recordOffset = recordOffset(currentVertex);
                var neighboursSizeOffset = recordOffset + diskCacheRecordEdgesCountOffset;
                var neighboursCount = Byte.toUnsignedInt(diskCache.get(ValueLayout.JAVA_BYTE, neighboursSizeOffset));
                var neighboursEnd = neighboursCount * Long.BYTES + diskCacheRecordEdgesOffset + recordOffset;


                for (var neighboursOffset = recordOffset + diskCacheRecordEdgesOffset;
                     neighboursOffset < neighboursEnd; neighboursOffset += Long.BYTES) {
                    var vertexIndex = diskCache.get(ValueLayout.JAVA_LONG, neighboursOffset);

                    if (visitedVertexIndices.add((int) vertexIndex)) {
                        testedVerticesSum++;

                        if (nearestCandidates.size() < maxAmountOfCandidates) {
                            var distance = computeDistance(diskCache,
                                    vectorOffset(vertexIndex), queryVector);
                            nearestCandidates.add((int) vertexIndex, distance, false);
                        } else {
                            if (lookupTable == null) {
                                lookupTable = buildPQDistanceLookupTable(queryVector);
                            }

                            var lastVertexDistance = nearestCandidates.maxDistance();
                            var pqDistance = computePQDistance(lookupTable, (int) vertexIndex);
                            if (lastVertexDistance >= pqDistance) {
                                nearestCandidates.add((int) vertexIndex, pqDistance, true);
                            }
                        }
                    }
                }

                assert nearestCandidates.size() <= maxAmountOfCandidates;
            }

            testedVerticesCount++;
            visitedVerticesCount++;

            visitedVerticesSum += visited;

            return nearestCandidates.vertexIndices(maxResultSize);
        }

        private long fetchId(long vertexIndex) {
            if (vertexIndex >= verticesSize) {
                throw new IllegalArgumentException();
            }

            var vertexPageIndex = vertexIndex / verticesPerPage;
            var vertexOffset = (vertexIndex % verticesPerPage) * vertexRecordSize + Long.BYTES;
            var vertexPageOffset = graphPages.get(vertexPageIndex);

            return diskCache.get(ValueLayout.JAVA_LONG,
                    vertexPageOffset + vertexOffset + diskCacheRecordIdOffset);
        }


        private long vectorOffset(long vertexIndex) {
            return recordOffset(vertexIndex) + diskCacheRecordVectorsOffset;
        }

        private long recordOffset(long vertexIndex) {
            if (vertexIndex >= verticesSize) {
                throw new IllegalArgumentException();
            }

            var vertexPageIndex = vertexIndex / verticesPerPage;
            var vertexPageOffset = graphPages.get(vertexPageIndex);
            var vertexOffset = (vertexIndex % verticesPerPage) * vertexRecordSize + Long.BYTES;
            return vertexPageOffset + vertexOffset;
        }

    }

    private static final class NearestGreedySearchCachedData {
        private final IntOpenHashSet visistedVertexIndices;
        private final float[] lookupTable;

        private final BoundedGreedyVertexPriorityQueue nearestCandidates;


        private NearestGreedySearchCachedData(IntOpenHashSet vertexIndices, float[] lookupTable, BoundedGreedyVertexPriorityQueue nearestCandidates) {
            this.visistedVertexIndices = vertexIndices;
            this.lookupTable = lookupTable;
            this.nearestCandidates = nearestCandidates;
        }
    }
}
