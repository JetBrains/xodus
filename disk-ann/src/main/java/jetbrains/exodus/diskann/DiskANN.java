package jetbrains.exodus.diskann;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.rng.sampling.PermutationSampler;
import org.apache.commons.rng.simple.RandomSource;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLongArray;

public final class DiskANN {
    public static final byte L2_DISTANCE = 0;
    public static final byte DOT_DISTANCE = 1;

    private static final VectorSpecies<Float> species = FloatVector.SPECIES_PREFERRED;

    private static final int PAGE_SIZE_MULTIPLIER = 4 * 1024;

    private static final VarHandle FLOAT_VIEW_VAR_HANDLE =
            MethodHandles.byteArrayViewVarHandle(float[].class, ByteOrder.nativeOrder());
    private static final VarHandle LONG_VIEW_VAR_HANDLE =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

    private static final Logger logger = LoggerFactory.getLogger(DiskANN.class);

    private final int vectorDim;

    private final float distanceMultiplication;

    private final int maxConnectionsPerVertex;

    private final int maxAmountOfCandidates;

    private final int mutatorsQueueSize;

    private long verticesSize = 0;
    private final Long2ObjectOpenHashMap<byte[]> graphPages = new Long2ObjectOpenHashMap<>();
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


    public DiskANN(String name, int vectorDim, byte distanceFunction) {
        this(name, vectorDim, distanceFunction, 2.1f,
                64, 128, 1024);
    }

    public DiskANN(String name, int vectorDim, byte distanceFunction,
                   float distanceMultiplication,
                   int maxConnectionsPerVertex,
                   int maxAmountOfCandidates, int mutatorsQueueSize) {
        this.vectorDim = vectorDim;
        this.distanceMultiplication = distanceMultiplication;
        this.maxConnectionsPerVertex = maxConnectionsPerVertex;
        this.maxAmountOfCandidates = maxAmountOfCandidates;
        this.mutatorsQueueSize = mutatorsQueueSize;
        this.distanceFunction = distanceFunction;

        this.vertexRecordSize = Float.BYTES * vectorDim + Long.BYTES * (maxConnectionsPerVertex + 1) + 1;

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

        var cores = Runtime.getRuntime().availableProcessors();
        if (logger.isInfoEnabled()) {
            logger.info("Using " + cores + " cores for mutation of vectors");
        }

        for (var i = 0; i < cores; i++) {
            vectorMutationThreads.add(Executors.newSingleThreadExecutor(r -> {
                var thread = new Thread(r, "$name - vector mutator-$i");
                thread.setDaemon(true);
                return thread;
            }));
        }
    }

    private long[] greedySearchNearest(
            DiskGraph graph,
            long startVertexIndex,
            float[] queryVertex,
            int maxResultSize
    ) {
        var nearestCandidates = new TreeSet<GreedyVertex>();
        var processingQueue = new PriorityQueue<GreedyVertex>();

        var visitedVertexIndices = new LongOpenHashSet();

        var startVector = graph.fetchVector(startVertexIndex);
        processingQueue.add(new GreedyVertex(startVertexIndex, computeDistance(startVector, queryVertex)));

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

            var vertexNeighbours = graph.fetchNeighbours(currentVertex.index);
            for (var vertexIndex : vertexNeighbours) {
                if (visitedVertexIndices.add(vertexIndex)) {
                    //return array and offset instead
                    var distance = computeDistance(queryVertex, graph.fetchVector(vertexIndex));
                    processingQueue.add(new GreedyVertex(vertexIndex, distance));
                }
            }

        }

        assert nearestCandidates.size() <= maxAmountOfCandidates;
        var resultSize = Math.min(maxResultSize, nearestCandidates.size());

        var nearestVertices = new long[resultSize];
        var nearestVerticesIterator = nearestCandidates.iterator();

        for (var i = 0; i < resultSize; i++) {
            nearestVertices[i] = nearestVerticesIterator.next().index;
        }

        return nearestVertices;
    }

    private void greedySearchPrune(
            InMemoryGraph graph,
            long startVertexIndex,
            long vertexIndexToPrune) {
        float[] vectors = graph.getVectors();

        var nearestCandidates = new TreeSet<GreedyVertex>();
        var processingQueue = new PriorityQueue<GreedyVertex>();

        var visitedVertexIndices = new LongOpenHashSet();


        var startVectorOffset = graph.vectorOffset(startVertexIndex);
        var queryVertexOffset = graph.vectorOffset(vertexIndexToPrune);
        var dim = vectorDim;

        processingQueue.add(new GreedyVertex(startVertexIndex, computeDistance(vectors, startVectorOffset,
                vectors, queryVertexOffset, dim)));

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

            var vertexNeighbours = graph.fetchNeighbours(currentVertex.index);
            for (var vertexIndex : vertexNeighbours) {
                if (visitedVertexIndices.add(vertexIndex)) {
                    //return array and offset instead
                    var distance = computeDistance(vectors, queryVertexOffset, vectors, graph.vectorOffset(vertexIndex),
                            dim);
                    processingQueue.add(new GreedyVertex(vertexIndex, distance));
                }
            }
        }

        assert nearestCandidates.size() <= maxAmountOfCandidates;
        robustPrune(graph, vertexIndexToPrune, visitedVertexIndices, distanceMultiplication);
    }

    private void robustPrune(
            InMemoryGraph graph,
            long vertexIndex,
            LongOpenHashSet neighboursCandidates,
            float distanceMultiplication
    ) {
        //TODO: use thread local containers for data instead
        //TODO: convert neighboursCandidates to long list
        var dim = vectorDim;
        graph.acquireVertex(vertexIndex);
        try {
            LongOpenHashSet candidates;
            if (graph.getNeighboursSize(vertexIndex) > 0) {
                var newCandidates = neighboursCandidates.clone();
                newCandidates.addAll(LongArrayList.wrap(graph.getNeighboursAndClear(vertexIndex)));

                candidates = newCandidates;
            } else {
                candidates = neighboursCandidates;
            }

            candidates.remove(vertexIndex);

            var vectors = graph.getVectors();
            var vectorOffset = graph.vectorOffset(vertexIndex);
            var candidatesIterator = candidates.longIterator();

            //TODO: use bounded min-max heap instead
            var cachedCandidates = new TreeSet<RobustPruneVertex>();
            while (candidatesIterator.hasNext()) {
                var candidateIndex = candidatesIterator.nextLong();

                var distance = computeDistance(vectors, vectorOffset, vectors,
                        graph.vectorOffset(candidateIndex), dim);
                var candidate = new RobustPruneVertex(candidateIndex, distance);

                cachedCandidates.add(candidate);
            }


            var neighbours = new LongArrayList(maxConnectionsPerVertex);
            var removed = new ArrayList<RobustPruneVertex>(cachedCandidates.size());

            var currentMultiplication = 1.0;
            neighboursLoop:
            while (currentMultiplication <= distanceMultiplication) {
                if (!removed.isEmpty()) {
                    //TODO: seems like candidates already sorted by distance, se we do not need to sort them again
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

                    var minOffset = graph.vectorOffset(min.index);
                    var iterator = cachedCandidates.iterator();
                    while (iterator.hasNext()) {
                        var candidate = iterator.next();
                        var distance = computeDistance(vectors, minOffset, vectors, graph.vectorOffset(candidate.index),
                                dim);

                        if (distance * currentMultiplication <= candidate.distance) {
                            iterator.remove();

                            if (distanceMultiplication > 1) {
                                removed.add(candidate);
                            }
                        }
                    }
                }

                currentMultiplication *= 1.2;
            }

            graph.setNeighbours(vertexIndex, neighbours.elements(), neighbours.size());
        } finally {
            graph.releaseVertex(vertexIndex);
        }
    }

    public void buildIndex(VectorReader vectorReader) {
        var size = vectorReader.size();
        var graph = new InMemoryGraph((int) size);

        for (var i = 0; i < size; i++) {
            var vertex = vectorReader.read(i);
            graph.addVector(vertex.leftLong(), vertex.right());
        }

        graph.generateRandomEdges();
        var medoid = graph.medoid();

        pruneIndexPass(size, graph, medoid, 1.0f);
        pruneIndexPass(size, graph, medoid, distanceMultiplication);

        graph.saveToDisk();

        diskGraph = new DiskGraph(medoid);
        verticesSize = size;
    }

    public long[] nearest(float[] vector, int resultSize) {
        var nearestVertices = greedySearchNearest(diskGraph, diskGraph.medoid(), vector,
                resultSize);
        var ids = new long[nearestVertices.length];

        for (var index = 0; index < nearestVertices.length; index++) {
            ids[index] = diskGraph.fetchId(nearestVertices[index]);
        }

        return ids;
    }

    private void pruneIndexPass(long size, InMemoryGraph graph, long medoid, float distanceMultiplication) {
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
                greedySearchPrune(graph, medoid, vertexIndex);

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
                                robustPrune(
                                        graph,
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

    private float computeDistance(float[] firstVector, float[] secondVector) {
        if (distanceFunction == L2_DISTANCE) {
            return computeL2Distance(firstVector, secondVector);
        } else if (distanceFunction == DOT_DISTANCE) {
            return computeDotDistance(firstVector, secondVector);
        } else {
            throw new IllegalStateException("Unknown distance function: " + distanceFunction);
        }
    }

    private float computeDistance(float[] firstVector,
                                  int firstVectorFrom,
                                  float[] secondVector,
                                  int secondVectorFrom,
                                  int size) {
        if (distanceFunction == L2_DISTANCE) {
            return computeL2Distance(firstVector, firstVectorFrom, secondVector, secondVectorFrom, size);
        } else if (distanceFunction == DOT_DISTANCE) {
            return computeDotDistance(firstVector, firstVectorFrom, secondVector, secondVectorFrom, size);
        } else {
            throw new IllegalStateException("Unknown distance function: " + distanceFunction);
        }
    }

    private static float computeL2Distance(float[] firstVector, float[] secondVector) {
        var sumVector = FloatVector.zero(species);
        var index = 0;

        while (index < species.loopBound(firstVector.length)) {
            var first = FloatVector.fromArray(species, firstVector, index);
            var second = FloatVector.fromArray(species, secondVector, index);
            var diff = first.sub(second);
            sumVector = diff.fma(diff, sumVector);
            index += species.length();
        }

        var sum = sumVector.reduceLanes(VectorOperators.ADD);

        while (index < firstVector.length) {
            var diff = firstVector[index] - secondVector[index];
            sum += diff * diff;
            index++;
        }

        return sum;
    }

    private static float computeL2Distance(float[] firstVector,
                                           int firstVectorFrom,
                                           float[] secondVector,
                                           int secondVectorFrom,
                                           int size) {
        var sumVector = FloatVector.zero(species);
        var index = 0;

        while (index < species.loopBound(size)) {
            var first = FloatVector.fromArray(species, firstVector, index + firstVectorFrom);
            var second = FloatVector.fromArray(species, secondVector, index + secondVectorFrom);
            var diff = first.sub(second);
            sumVector = diff.fma(diff, sumVector);
            index += species.length();
        }

        var sum = sumVector.reduceLanes(VectorOperators.ADD);

        while (index < size) {
            var diff = firstVector[index + firstVectorFrom] - secondVector[index + secondVectorFrom];
            sum += diff * diff;
            index++;
        }

        return sum;
    }

    private static float computeDotDistance(float[] firstVector, float[] secondVector) {
        var sumVector = FloatVector.zero(species);
        var index = 0;

        while (index < species.loopBound(firstVector.length)) {
            var first = FloatVector.fromArray(species, firstVector, index);
            var second = FloatVector.fromArray(species, secondVector, index);
            sumVector = first.fma(second, sumVector);
            index += species.length();
        }

        var sum = sumVector.reduceLanes(VectorOperators.ADD);

        while (index < firstVector.length) {
            sum += firstVector[index] * secondVector[index];
            index++;
        }

        return -sum;
    }

    private static float computeDotDistance(float[] firstVector,
                                            int firstVectorFrom,
                                            float[] secondVector,
                                            int secondVectorFrom,
                                            int size) {
        var sumVector = FloatVector.zero(species);
        var index = 0;

        while (index < species.loopBound(size)) {
            var first = FloatVector.fromArray(species, firstVector, index + firstVectorFrom);
            var second = FloatVector.fromArray(species, secondVector, index + secondVectorFrom);
            sumVector = first.fma(second, sumVector);
            index += species.length();
        }

        var sum = sumVector.reduceLanes(VectorOperators.ADD);

        while (index < size) {
            sum += firstVector[index + firstVectorFrom] * secondVector[index + secondVectorFrom];
            index++;
        }

        return -sum;
    }


    private final class InMemoryGraph {

        int size = 0;

        private final float[] vectors;
        private final long[] ids;

        private final long[] edges;
        private final AtomicLongArray edgeVersions;

        private long medoid = -1;

        private InMemoryGraph(int capacity) {
            this.vectors = new float[vectorDim * capacity];
            this.ids = new long[capacity];
            this.edges = new long[(maxConnectionsPerVertex + 1) * capacity];
            edgeVersions = new AtomicLongArray(capacity);
        }


        void addVector(long id, float[] vector) {
            var offset = size * vectorDim;

            System.arraycopy(vector, 0, vectors, offset, vectorDim);

            ids[size] = id;
            size++;

            medoid = -1;
        }

        int vectorOffset(long vertexIndex) {
            return (int) (vertexIndex * vectorDim);
        }

        float[] getVectors() {
            return vectors;
        }

        int getNeighboursSize(long vertexIndex) {
            var version = edgeVersions.get((int) vertexIndex);
            while (true) {
                var size = (int) edges[(int) (vertexIndex * (maxConnectionsPerVertex + 1))];
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
        public float[] fetchVector(long vertexIndex) {
            int start = (int) (vertexIndex * vectorDim);
            int end = start + vectorDim;

            return Arrays.copyOfRange(vectors, start, end);
        }

        @NotNull
        public long[] fetchNeighbours(long vertexIndex) {
            var version = edgeVersions.get((int) vertexIndex);

            while (true) {
                var edgesOffset = (int) (vertexIndex * (maxConnectionsPerVertex + 1));
                var size = (int) edges[edgesOffset];


                var result = Arrays.copyOfRange(edges, edgesOffset + 1, edgesOffset + 1 + size);
                var newVersion = edgeVersions.get((int) vertexIndex);

                VarHandle.acquireFence();
                if (newVersion == version) {
                    assert (size >= 0 && size <= maxConnectionsPerVertex);
                    return result;
                }

                version = newVersion;
            }
        }

        void setNeighbours(long vertexIndex, long[] neighbours, int size) {
            validateLocked(vertexIndex);
            assert (size >= 0 && size <= maxConnectionsPerVertex);

            var edgesOffset = (int) (vertexIndex * (maxConnectionsPerVertex + 1));

            edges[edgesOffset] = size;

            System.arraycopy(neighbours, 0, edges, edgesOffset + 1, size);
        }

        void appendNeighbour(long vertexIndex, long neighbour) {
            validateLocked(vertexIndex);

            var edgesOffset = (int) (vertexIndex * (maxConnectionsPerVertex + 1));
            var size = (int) edges[edgesOffset];

            assert size + 1 <= maxConnectionsPerVertex;

            edges[edgesOffset] = size + 1;
            edges[edgesOffset + size + 1] = neighbour;
        }


        void generateRandomEdges() {
            if (size == 1) {
                return;
            }

            var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
            var shuffledIndexes = PermutationSampler.natural(size);
            PermutationSampler.shuffle(rng, shuffledIndexes);

            var maxEdges = Math.min(size - 1, maxConnectionsPerVertex);
            var shuffleIndex = 0;
            for (var i = 0; i < size; i++) {
                var edgesOffset = i * (maxConnectionsPerVertex + 1);
                edges[edgesOffset] = maxEdges;

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

                    edges[++edgesOffset] = randomIndex;
                    addedEdges++;
                }
            }
        }

        long[] getNeighboursAndClear(long vertexIndex) {
            validateLocked(vertexIndex);
            var edgesOffset = (int) (vertexIndex * (maxConnectionsPerVertex + 1));
            var result = fetchNeighbours(vertexIndex);

            edgeVersions.incrementAndGet((int) vertexIndex);
            edges[edgesOffset] = 0L;
            edgeVersions.incrementAndGet((int) vertexIndex);

            return result;
        }

        public long medoid() {
            if (medoid == -1L) {
                medoid = calculateMedoid();
            }

            return medoid;
        }

        public void acquireVertex(long vertexIndex) {
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

        public void releaseVertex(long vertexIndex) {
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
                var vector = fetchVector(i);
                for (var j = 0; j < vectorDim; j++) {
                    meanVector[j] += vector[j];
                }
            }

            for (var j = 0; j < vectorDim; j++) {
                meanVector[j] = meanVector[j] / size;
            }

            var minDistance = Double.POSITIVE_INFINITY;
            var medoidIndex = -1;

            for (var i = 0; i < size; i++) {
                var distance = computeDistance(
                        vectors,
                        i * vectorDim, meanVector, 0, vectorDim
                );

                if (distance < minDistance) {
                    minDistance = distance;
                    medoidIndex = i;
                }
            }

            return medoidIndex;
        }


        void saveToDisk() {
            var verticesPerPage = pageSize / vertexRecordSize;

            var wittenVertices = 0;

            var vectorsOffset = 0;
            var pageIndex = 0;

            while (wittenVertices < size) {
                var page = new byte[pageSize];
                LONG_VIEW_VAR_HANDLE.set(page, 0, size);

                var verticesToWrite = Math.min(verticesPerPage, size - wittenVertices);


                for (var i = 0; i < verticesToWrite; i++) {
                    var edgesOffset = wittenVertices * (maxConnectionsPerVertex + 1);
                    var dataOffset = Long.BYTES + i * vertexRecordSize;

                    for (var j = 0; j < vectorDim; j++) {
                        FLOAT_VIEW_VAR_HANDLE.set(page, dataOffset, vectors[vectorsOffset]);
                        vectorsOffset++;
                        dataOffset += Float.BYTES;
                    }

                    LONG_VIEW_VAR_HANDLE.set(page, dataOffset, ids[wittenVertices]);
                    dataOffset += Long.BYTES;

                    var edgesSize = edges[edgesOffset];
                    assert (edgesSize >= 0 && edgesSize <= maxConnectionsPerVertex);
                    edgesOffset++;
                    page[dataOffset++] = (byte) edgesSize;

                    for (var j = 0; j < edgesSize; j++) {
                        LONG_VIEW_VAR_HANDLE.set(page, dataOffset, edges[edgesOffset]);
                        edgesOffset++;
                        dataOffset += Long.BYTES;
                    }

                    wittenVertices++;
                }

                graphPages.put(pageIndex, page);
                pageIndex++;
            }
        }
    }

    private final class DiskGraph {
        private final long medoid;

        private DiskGraph(long medoid) {
            this.medoid = medoid;
        }

        public long medoid() {
            return medoid;
        }

        public long fetchId(long vertexIndex) {
            if (vertexIndex >= verticesSize) {
                throw new IllegalArgumentException();
            }

            var vertexPageIndex = vertexIndex / verticesPerPage;
            var vertexOffset = (vertexIndex % verticesPerPage) * vertexRecordSize + Long.BYTES;

            var vertexPage = graphPages.get(vertexPageIndex);
            return (long) LONG_VIEW_VAR_HANDLE.get(vertexPage, (int) (vertexOffset + (long) vectorDim * Float.BYTES));
        }


        @NotNull
        public float[] fetchVector(long vertexIndex) {
            if (vertexIndex >= verticesSize) {
                throw new IllegalArgumentException();
            }

            var vertexPageIndex = vertexIndex / verticesPerPage;
            var vertexPage = graphPages.get(vertexPageIndex);
            var vertexOffset = (vertexIndex % verticesPerPage) * vertexRecordSize + Long.BYTES;
            return fetchVectorData(vertexPage, (int) vertexOffset);
        }

        private float[] fetchVectorData(byte[] page, int offset) {
            var vectorData = new float[vectorDim];

            for (var i = 0; i < vectorDim; i++) {
                vectorData[i] = (float) FLOAT_VIEW_VAR_HANDLE.get(page, offset + i * Float.BYTES);
            }

            return vectorData;
        }

        @NotNull
        public long[] fetchNeighbours(long vertexIndex) {
            if (vertexIndex >= verticesSize) {
                throw new IllegalArgumentException();
            }

            var vertexPageIndex = vertexIndex / verticesPerPage;
            var vertexPage = graphPages.get(vertexPageIndex);

            var vertexOffset =
                    (int) (vertexIndex % verticesPerPage) * vertexRecordSize +
                            vectorDim * Float.BYTES + Long.BYTES + 1 + Long.BYTES;


            var neighboursSize = Byte.toUnsignedInt(vertexPage[vertexOffset - 1]);
            assert (neighboursSize <= maxConnectionsPerVertex);

            var result = new long[neighboursSize];
            for (var i = 0; i < neighboursSize; i++) {
                result[i] = (long)
                        LONG_VIEW_VAR_HANDLE.get(vertexPage, vertexOffset + i * Long.BYTES);
            }

            return result;
        }

    }
}
