package jetbrains.exodus.diskann;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import jdk.incubator.vector.FloatVector;
import jetbrains.exodus.diskann.objectpool.ObjectProducer;
import jetbrains.exodus.diskann.objectpool.UnboundedObjectPool;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.rng.sampling.PermutationSampler;
import org.apache.commons.rng.simple.RandomSource;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLongArray;

public final class DiskANN {
    private static final int PAGE_SIZE_MULTIPLIER = 4 * 1024;

    private static final VarHandle FLOAT_VIEW_VAR_HANDLE =
            MethodHandles.byteArrayViewVarHandle(float[].class, ByteOrder.nativeOrder());
    private static final VarHandle LONG_VIEW_VAR_HANDLE =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

    private static final Logger logger = LoggerFactory.getLogger(DiskANN.class);

    private final int vectorDim;
    private final DistanceFunction distanceFunction;
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
    private Graph diskGraph;


    private final UnboundedObjectPool<LongOpenHashSet> visitedVerticesIndexPool =
            new UnboundedObjectPool<>(new ObjectProducer<>() {
                @Override
                public LongOpenHashSet produce() {
                    return new LongOpenHashSet();
                }

                @Override
                public void clear(LongOpenHashSet longOpenHashSet) {
                    longOpenHashSet.clear();
                }
            });

    private final UnboundedObjectPool<Long2LongOpenHashMap> visitedVertexVersionsPoll =
            new UnboundedObjectPool<>(new ObjectProducer<>() {
                @Override
                public Long2LongOpenHashMap produce() {
                    return new Long2LongOpenHashMap();
                }

                @Override
                public void clear(Long2LongOpenHashMap long2LongOpenHashMap) {
                    long2LongOpenHashMap.clear();
                }
            });

    public DiskANN(String name, int vectorDim, DistanceFunction distanceFunction) {
        this(name, vectorDim, distanceFunction, 2.1f,
                64, 128, 1024);
    }

    public DiskANN(String name, int vectorDim, DistanceFunction distanceFunction,
                   float distanceMultiplication,
                   int maxConnectionsPerVertex,
                   int maxAmountOfCandidates, int mutatorsQueueSize) {
        this.vectorDim = vectorDim;
        this.distanceFunction = distanceFunction;
        this.distanceMultiplication = distanceMultiplication;
        this.maxConnectionsPerVertex = maxConnectionsPerVertex;
        this.maxAmountOfCandidates = maxAmountOfCandidates;
        this.mutatorsQueueSize = mutatorsQueueSize;

        this.vertexRecordSize = Float.BYTES * vectorDim + Long.BYTES * (maxConnectionsPerVertex + 1) + 1;

        if (this.vertexRecordSize > PAGE_SIZE_MULTIPLIER - 1) {
            this.pageSize = ((vertexRecordSize + PAGE_SIZE_MULTIPLIER - 1 - Long.BYTES) /
                    (PAGE_SIZE_MULTIPLIER - Long.BYTES)) * PAGE_SIZE_MULTIPLIER;
        } else {
            this.pageSize = PAGE_SIZE_MULTIPLIER;
        }

        this.verticesPerPage = (pageSize - Long.BYTES) / vertexRecordSize;

        var species = FloatVector.SPECIES_PREFERRED;
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
            Graph graph,
            long startVertexIndex,
            float[] queryVertex,
            int maxResultSize
    ) {
        var nearestCandidates = new TreeSet<GreedyVertex>();
        var visitedVertexIndices = visitedVerticesIndexPool.borrowObject();

        try {
            var startVector = graph.fetchVector(startVertexIndex);
            var distanceFunction = this.distanceFunction;

            var startVertex = new GreedyVertex(
                    startVertexIndex, distanceFunction.computeDistance(queryVertex, startVector));

            nearestCandidates.add(startVertex);
            var candidatesToVisit = 1;

            while (candidatesToVisit > 0) {
                assert candidatesToVisit <= maxAmountOfCandidates;
                assert assertNearestCandidates(nearestCandidates, candidatesToVisit);

                var minVertexIterator = nearestCandidates.iterator();

                GreedyVertex minVertex = null;

                while (minVertexIterator.hasNext()) {
                    minVertex = minVertexIterator.next();
                    if (!minVertex.visited) {
                        break;
                    }
                }

                assert minVertex != null;
                visitedVertexIndices.add(minVertex.index);

                minVertex.visited = true;
                candidatesToVisit--;

                candidatesToVisit = filterNeighbours(graph, queryVertex, nearestCandidates,
                        visitedVertexIndices, distanceFunction, candidatesToVisit, minVertex);
            }


            assert candidatesToVisit == 0;
            assert nearestCandidates.size() <= maxAmountOfCandidates;

            var resultSize = Math.min(maxResultSize, nearestCandidates.size());
            var nearestVertices = new long[resultSize];
            var nearestVerticesIterator = nearestCandidates.iterator();

            for (var i = 0; i < resultSize; i++) {
                nearestVertices[i] = nearestVerticesIterator.next().index;
            }

            return nearestVertices;
        } finally {
            visitedVerticesIndexPool.returnObject(visitedVertexIndices);
        }
    }

    private int filterNeighbours(Graph graph, float[] queryVertex, TreeSet<GreedyVertex> nearestCandidates,
                                 LongOpenHashSet visitedVertexIndices, DistanceFunction distanceFunction,
                                 int candidatesToVisit, GreedyVertex minVertex) {
        var vertexNeighbours = graph.fetchNeighbours(minVertex.index);
        for (var vertexIndex : vertexNeighbours) {
            if (visitedVertexIndices.contains(vertexIndex)) {
                continue;
            }

            var vertexVector = graph.fetchVector(vertexIndex);
            var vertex =
                    new GreedyVertex(vertexIndex, distanceFunction.computeDistance(queryVertex, vertexVector));
            if (nearestCandidates.add(vertex)) {
                candidatesToVisit++;

                while (nearestCandidates.size() > maxAmountOfCandidates) {
                    var removed = nearestCandidates.pollLast();

                    assert removed != null;
                    if (!removed.visited) {
                        candidatesToVisit--;
                    }
                }
            }

            assert candidatesToVisit >= 0 && candidatesToVisit <= maxAmountOfCandidates;
        }
        return candidatesToVisit;
    }

    private LongOpenHashSet greedySearchPrune(
            Graph graph,
            long startVertexIndex,
            float[] queryVertex
    ) {
        var nearestCandidates = new TreeSet<GreedyVertex>();
        var visitedVertexIndices = visitedVerticesIndexPool.borrowObject();
        var vertexVersions = visitedVertexVersionsPoll.borrowObject();


        try {
            traverseLoop:
            while (true) {
                var startVector = graph.fetchVector(startVertexIndex);
                var distanceFunction = this.distanceFunction;

                var startVertex = new GreedyVertex(
                        startVertexIndex, distanceFunction.computeDistance(queryVertex, startVector));

                nearestCandidates.add(startVertex);
                var candidatesToVisit = 1;

                while (candidatesToVisit > 0) {
                    assert candidatesToVisit <= maxAmountOfCandidates;
                    assert assertNearestCandidates(nearestCandidates, candidatesToVisit);

                    var minVertexIterator = nearestCandidates.iterator();

                    GreedyVertex minVertex = null;

                    while (minVertexIterator.hasNext()) {
                        minVertex = minVertexIterator.next();
                        if (!minVertex.visited) {
                            break;
                        }
                    }

                    assert minVertex != null;
                    visitedVertexIndices.add(minVertex.index);
                    vertexVersions.put(minVertex.index, graph.vertexVersion(minVertex.index));

                    minVertex.visited = true;
                    candidatesToVisit--;

                    candidatesToVisit = filterNeighbours(graph, queryVertex, nearestCandidates,
                            visitedVertexIndices, distanceFunction, candidatesToVisit, minVertex);
                }

                var vertexVersionsIterator = vertexVersions.long2LongEntrySet().fastIterator();
                while (vertexVersionsIterator.hasNext()) {
                    var vertexVersionEntry = vertexVersionsIterator.next();
                    var vertexIndex = vertexVersionEntry.getLongKey();
                    var vertexVersion = vertexVersionEntry.getLongValue();
                    if (graph.validateVertexVersion(vertexIndex, vertexVersion)) {
                        break traverseLoop;
                    }
                }
                nearestCandidates.clear();
                visitedVertexIndices.clear();
                vertexVersions.clear();
            }

            assert nearestCandidates.size() <= maxAmountOfCandidates;

            return visitedVertexIndices.clone();
        } finally {
            visitedVertexVersionsPoll.returnObject(vertexVersions);
        }
    }

    private void robustPrune(
            InMemoryGraph graph,
            long vertexIndex,
            LongOpenHashSet neighboursCandidates,
            float distanceMultiplication
    ) {
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

            var vertexVector = graph.fetchVector(vertexIndex);
            var candidatesIterator = candidates.longIterator();

            var cachedCandidates = new TreeSet<RobustPruneVertex>();
            while (candidatesIterator.hasNext()) {
                var candidateIndex = candidatesIterator.nextLong();
                var candidateVector = graph.fetchVector(candidateIndex);

                var distance = distanceFunction.computeDistance(vertexVector, candidateVector);
                var candidate = new RobustPruneVertex(candidateIndex, candidateVector, distance);

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

                    var iterator = cachedCandidates.iterator();
                    while (iterator.hasNext()) {
                        var candidate = iterator.next();
                        var distance = distanceFunction.computeDistance(min.vector, candidate.vector);

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

    private boolean assertNearestCandidates(SortedSet<GreedyVertex> nearestCandidates, int candidatesToVisit) {
        var vertexIterator = nearestCandidates.iterator();
        var candidates = 0;

        while (vertexIterator.hasNext()) {
            var vertex = vertexIterator.next();
            if (!vertex.visited) {
                candidates++;
            }
        }

        assert candidates == candidatesToVisit;
        return true;
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

        if (logger.isDebugEnabled()) {
            logger.info("Graph pruning started with distance multiplication " + distanceMultiplication + ".");
        }

        var mutatorFutures = new ArrayList<Future<ArrayList<Future<?>>>>();
        for (var n = 0; n < permutation.length; n++) {
            var vertexIndex = permutation[n];
            var mutatorIndex = (vertexIndex % vectorMutationThreads.size());
            var mutator = vectorMutationThreads.get(mutatorIndex);

            var mutatorFuture = mutator.submit(() -> {
                var visited = greedySearchPrune(graph, medoid, graph.fetchVector(vertexIndex));
                try {
                    robustPrune(graph, vertexIndex, visited, distanceMultiplication);
                } finally {
                    visitedVerticesIndexPool.returnObject(visited);
                }


                var features = new ArrayList<Future<?>>(maxConnectionsPerVertex + 1);
                var neighbours = graph.fetchNeighbours(vertexIndex);
                for (var neighbour : neighbours) {
                    var neighbourMutatorIndex = (int) (neighbour % vectorMutationThreads.size());
                    var neighbourMutator = vectorMutationThreads.get(neighbourMutatorIndex);

                    var neighborMutatorFuture = neighbourMutator.submit(() -> {
                        var neighbourNeighbours = graph.fetchNeighbours(neighbour);


                        if (!ArrayUtils.contains(neighbourNeighbours, vertexIndex)) {
                            if (neighbourNeighbours.length + 1 <= maxConnectionsPerVertex) {
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
                        }
                    });
                    features.add(neighborMutatorFuture);
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
                if (logger.isInfoEnabled()) {
                    logger.info("Graph pruning: " + (n + 1) + " vertices out of " + size + " were processed.");
                }

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


    private final class InMemoryGraph implements Graph {

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

        int getNeighboursSize(long vertexIndex) {
            var size = (int) edges[(int) (vertexIndex * (maxConnectionsPerVertex + 1))];
            assert size >= 0 && size <= maxConnectionsPerVertex;
            return size;
        }


        public long fetchId(long vertexIndex) {
            return ids[(int) vertexIndex];
        }

        @NotNull
        public float[] fetchVector(long vertexIndex) {
            int start = (int) (vertexIndex * vectorDim);
            int end = start + vectorDim;

            return Arrays.copyOfRange(vectors, start, end);
        }

        @NotNull
        public long[] fetchNeighbours(long vertexIndex) {
            var edgesOffset = (int) (vertexIndex * (maxConnectionsPerVertex + 1));
            var size = (int) edges[edgesOffset];

            assert (size >= 0 && size <= maxConnectionsPerVertex);
            return Arrays.copyOfRange(edges, edgesOffset + 1, edgesOffset + 1 + size);
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

        public long vertexVersion(long vertexIndex) {
            return edgeVersions.get((int) vertexIndex);
        }

        public boolean validateVertexVersion(long vertexIndex, long version) {
            VarHandle.acquireFence();
            return (edgeVersions.get((int) vertexIndex) == version) && ((version & 1L) == 0L);
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
                var distance = distanceFunction.computeDistance(
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

    private final class DiskGraph implements Graph {
        private final long medoid;

        private DiskGraph(long medoid) {
            this.medoid = medoid;
        }

        public long medoid() {
            return medoid;
        }

        public long vertexVersion(long vertexIndex) {
            return 0;
        }

        public boolean validateVertexVersion(long vertexIndex, long version) {
            return true;
        }

        public void acquireVertex(long vertexIndex) {
        }

        public void releaseVertex(long vertexIndex) {
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
