package jetbrains.exodus.diskann

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.longs.LongArrayList
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import jdk.incubator.vector.FloatVector
import jdk.incubator.vector.VectorSpecies
import mu.KLogging
import org.apache.commons.rng.sampling.PermutationSampler
import org.apache.commons.rng.simple.RandomSource
import java.lang.invoke.MethodHandles
import java.lang.invoke.VarHandle
import java.nio.ByteOrder
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicLongArray
import kotlin.collections.ArrayList
import kotlin.math.min

const val PAGE_SIZE_MULTIPLIER = 4 * 1024

internal class DiskANN(
    private val name: String,
    private val vectorDim: Int, private val distanceFunction: DistanceFunction,
    private val distanceMultiplication: Float = 2.1f, private val maxConnectionsPerVertex: Int = 64,
    private val maxAmountOfCandidates: Int = 128, private val mutatorsQueueSize: Int = 1024,
) {
    private var verticesSize: Long = 0
    private val graphPages: Long2ObjectOpenHashMap<ByteArray> = Long2ObjectOpenHashMap()
    private val vectorMutationThreads = ArrayList<ExecutorService>()

    /**
     * Size of vertex record in bytes.
     *
     * 1. Vector data (4 bytes * vectorDim)
     * 2. Vector id (4 bytes)
     * 3. Real amount of edges (1 byte)
     * 4. Edges to other vertices (4 bytes * maxConnectionsPerVertex)
     */
    private val vertexRecordSize = Float.SIZE_BYTES * vectorDim + Long.SIZE_BYTES * (maxConnectionsPerVertex + 1) + 1

    /**
     * During calculation of the amount of vertices per page we need to take into account that first byte of
     * each page contains amount of vertices in the index.
     */
    private val pageSize: Int = if (vertexRecordSize > PAGE_SIZE_MULTIPLIER - 1) {
        ((vertexRecordSize + PAGE_SIZE_MULTIPLIER - 1 - Long.SIZE_BYTES) /
                (PAGE_SIZE_MULTIPLIER - Long.SIZE_BYTES)) * PAGE_SIZE_MULTIPLIER
    } else {
        PAGE_SIZE_MULTIPLIER
    }

    private val verticesPerPage: Int = (pageSize - Long.SIZE_BYTES) / vertexRecordSize
    private var diskGraph: Graph? = null

    init {
        val species: VectorSpecies<Float> = FloatVector.SPECIES_PREFERRED
        logger.info {
            "Vector index '$name' has been initialized. Vector lane count for distance calculation " +
                    "is ${species.length()}"
        }
        val cores = Runtime.getRuntime().availableProcessors()
        logger.info { "Using $cores cores for mutation of vectors" }

        for (i in 0 until cores) {
            vectorMutationThreads.add(Executors.newSingleThreadExecutor { r ->
                val thread = Thread(r, "$name - vector mutator-$i")
                thread.isDaemon = true
                thread
            })
        }

    }

    private fun greedySearch(
        graph: Graph,
        startVertexIndex: Long,
        queryVertex: FloatArray,
        maxResultSize: Int
    ): Pair<LongArray, LongOpenHashSet> {
        val nearestCandidates = TreeSet<GreedyVertex>()

        val visitedVertexIndices = LongOpenHashSet(maxAmountOfCandidates)

        val startVector = graph.fetchVector(startVertexIndex)
        val distanceFunction = distanceFunction

        val startVertex = GreedyVertex(
            startVertexIndex, distanceFunction.computeDistance(queryVertex, startVector)
        )

        nearestCandidates.add(startVertex)
        var candidatesToVisit = 1

        while (candidatesToVisit > 0) {
            assert(candidatesToVisit in 0..maxAmountOfCandidates)
            assert(assertNearestCandidates(nearestCandidates, candidatesToVisit))

            val minVertexIterator = nearestCandidates.iterator()

            var minVertex: GreedyVertex? = null

            while (minVertexIterator.hasNext()) {
                minVertex = minVertexIterator.next()
                if (!minVertex.visited) {
                    break
                }
            }

            visitedVertexIndices.add(minVertex!!.index)

            minVertex.visited = true
            candidatesToVisit--

            val vertexNeighbours = graph.fetchNeighbours(minVertex.index)
            for (vertexIndex in vertexNeighbours) {
                if (visitedVertexIndices.contains(vertexIndex)) {
                    continue
                }

                val vertexVector = graph.fetchVector(vertexIndex)
                val vertex =
                    GreedyVertex(vertexIndex, distanceFunction.computeDistance(queryVertex, vertexVector))
                if (nearestCandidates.add(vertex)) {
                    candidatesToVisit++

                    while (nearestCandidates.size > maxAmountOfCandidates) {
                        val removed = nearestCandidates.pollLast()!!

                        if (!removed.visited) {
                            candidatesToVisit--
                        }
                    }
                }

                assert(candidatesToVisit in 0..maxAmountOfCandidates)
            }
        }

        assert(nearestCandidates.size <= maxAmountOfCandidates)

        val resultSize = min(maxResultSize, nearestCandidates.size)
        val nearestVertices = LongArray(resultSize)
        val nearestVerticesIterator = nearestCandidates.iterator()

        for (i in 0 until resultSize) {
            nearestVertices[i] = nearestVerticesIterator.next().index
        }

        return Pair(nearestVertices, visitedVertexIndices)
    }

    private fun assertNearestCandidates(nearestCandidates: TreeSet<GreedyVertex>, candidatesToVisit: Int): Boolean {
        val vertexIterator = nearestCandidates.iterator()
        var candidates = 0
        while (vertexIterator.hasNext()) {
            val vertex = vertexIterator.next()
            if (!vertex.visited) {
                candidates++
            }
        }

        assert(candidates == candidatesToVisit)
        return true
    }

    private fun robustPrune(
        graph: InMemoryGraph,
        vertexIndex: Long,
        neighboursCandidates: LongOpenHashSet,
        distanceMultiplication: Float
    ) {
        val candidates = if (graph.getNeighboursSize(vertexIndex) > 0) {
            val newCandidates = neighboursCandidates.clone()
            newCandidates.addAll(LongArrayList.wrap(graph.getNeighboursAndClear(vertexIndex)))

            newCandidates
        } else {
            neighboursCandidates
        }

        candidates.remove(vertexIndex)

        val vertexVector = graph.fetchVector(vertexIndex)
        val candidatesIterator = candidates.longIterator()

        val cachedCandidates = TreeSet<RobustPruneVertex>()
        while (candidatesIterator.hasNext()) {
            val candidateIndex = candidatesIterator.nextLong()
            val candidateVector = graph.fetchVector(candidateIndex)

            val distance = distanceFunction.computeDistance(vertexVector, candidateVector)
            val candidate = RobustPruneVertex(candidateIndex, candidateVector, distance)

            cachedCandidates.add(candidate)
        }


        val neighbours = LongArrayList(maxConnectionsPerVertex)
        val removed = ArrayList<RobustPruneVertex>(cachedCandidates.size)

        var currentMultiplication = 1.0
        neighboursLoop@ while (currentMultiplication <= distanceMultiplication) {
            if (removed.isNotEmpty()) {
                cachedCandidates.addAll(removed)
                removed.clear()
            }

            while (cachedCandidates.isNotEmpty()) {
                val min = cachedCandidates.pollFirst()!!
                neighbours.add(min.index)

                if (neighbours.size == maxConnectionsPerVertex) {
                    break@neighboursLoop
                }

                val iterator = cachedCandidates.iterator()
                while (iterator.hasNext()) {
                    val candidate = iterator.next()
                    val distance = distanceFunction.computeDistance(min.vector, candidate.vector)

                    if (distance * currentMultiplication <= candidate.distance) {
                        iterator.remove()

                        if (distanceMultiplication > 1) {
                            removed.add(candidate)
                        }
                    }
                }
            }

            currentMultiplication *= 1.2
        }

        graph.setNeighbours(vertexIndex, neighbours.elements(), neighbours.size)
    }

    fun buildIndex(vectorReader: VectorReader) {
        val size = vectorReader.size()
        val graph = InMemoryGraph(size.toInt())

        for (i in 0 until size) {
            val vertex = vectorReader.read(i)
            graph.addVector(vertex.first, vertex.second)
        }

        graph.generateRandomEdges()
        val medoid = graph.medoid()

        pruneIndexPass(size, graph, medoid, 1.0f)
        pruneIndexPass(size, graph, medoid, distanceMultiplication)

        graph.saveToDisk()

        diskGraph = DiskGraph(medoid)
        verticesSize = size
    }

    fun nearest(vector: FloatArray, resultSize: Int): LongArray {
        val diskGraph = diskGraph!!

        val (nearestVertices, _) = greedySearch(diskGraph, diskGraph.medoid(), vector, resultSize)
        val ids = LongArray(nearestVertices.size)

        for (index in nearestVertices.indices) {
            ids[index] = diskGraph.fetchId(nearestVertices[index])
        }

        return ids
    }

    private fun pruneIndexPass(size: Long, graph: InMemoryGraph, medoid: Long, distanceMultiplication: Float) {
        val rng = RandomSource.XO_RO_SHI_RO_128_PP.create()
        val permutation = PermutationSampler(rng, size.toInt(), size.toInt()).sample()

        logger.info { "Graph pruning started with distance multiplication $distanceMultiplication." }


        val mutatorFutures = ArrayList<Future<ArrayList<Future<*>>>>()
        for (n in permutation.indices) {
            val vertexIndex = permutation[n].toLong()
            val mutatorIndex = (vertexIndex % vectorMutationThreads.size).toInt()
            val mutator = vectorMutationThreads[mutatorIndex]

            val mutatorFuture = mutator.submit(Callable {
                val (_, visited) = greedySearch(graph, medoid, graph.fetchVector(vertexIndex), 1)
                robustPrune(graph, vertexIndex, visited, distanceMultiplication)

                val features = ArrayList<Future<*>>(maxConnectionsPerVertex + 1)
                val neighbours = graph.fetchNeighbours(vertexIndex)
                for (neighbour in neighbours) {
                    val neighbourMutatorIndex = (neighbour % vectorMutationThreads.size).toInt()
                    val neighbourMutator = vectorMutationThreads[neighbourMutatorIndex]

                    val neighborMutatorFuture = neighbourMutator.submit {
                        val neighbourNeighbours = graph.fetchNeighbours(neighbour)

                        if (!neighbourNeighbours.contains(vertexIndex)) {
                            if (neighbourNeighbours.size + 1 <= maxConnectionsPerVertex) {
                                graph.appendNeighbour(neighbour, vertexIndex)
                            } else {
                                robustPrune(
                                    graph,
                                    neighbour,
                                    LongOpenHashSet(longArrayOf(vertexIndex)),
                                    distanceMultiplication
                                )
                            }
                        }
                    }
                    features.add(neighborMutatorFuture)
                }

                features
            })

            mutatorFutures.add(mutatorFuture)

            if (mutatorFutures.size == mutatorsQueueSize) {
                for (feature in mutatorFutures) {
                    val subFutures = feature.get()
                    for (subFuture in subFutures) {
                        subFuture.get()
                    }
                }
            }

            if ((n + 1) % 1000L == 0L) {
                logger.info { "Graph pruning: ${n + 1} vertices out of $size were processed." }
            }
        }
    }


    companion object : KLogging() {
        val FLOAT_VIEW_VAR_HANDLE: VarHandle =
            MethodHandles.byteArrayViewVarHandle(FloatArray::class.java, ByteOrder.nativeOrder())
        val LONG_VIEW_VAR_HANDLE: VarHandle =
            MethodHandles.byteArrayViewVarHandle(LongArray::class.java, ByteOrder.nativeOrder())
    }

    private inner class InMemoryGraph(capacity: Int) : Graph {
        @JvmField
        var size: Int = 0

        private val vectors: FloatArray = FloatArray(vectorDim * capacity)
        private val ids: LongArray = LongArray(capacity)

        private val edges = LongArray((maxConnectionsPerVertex + 1) * capacity)
        private val edgeVersions = AtomicLongArray(capacity)

        private var medoid: Long = -1

        fun addVector(id: Long, vector: FloatArray) {
            val offset = size * vectorDim
            vector.copyInto(vectors, offset, 0, vectorDim)

            ids[size] = id
            size++

            medoid = -1
        }

        fun getNeighboursSize(vertexIndex: Long): Int {
            var initVersion = edgeVersions[vertexIndex.toInt()]

            while (true) {
                val size = edges[(vertexIndex * (maxConnectionsPerVertex + 1)).toInt()].toInt()
                assert(size in 0..maxConnectionsPerVertex)
                val finalVersion = edgeVersions[vertexIndex.toInt()]

                if (validateVersion(finalVersion, initVersion)) {
                    return size
                }
                initVersion = finalVersion
            }
        }

        private fun validateVersion(finalVersion: Long, initVersion: Long) =
            finalVersion == initVersion && initVersion and 1.toLong() == 0.toLong()

        override fun fetchId(vertexIndex: Long): Long {
            return ids[vertexIndex.toInt()]
        }

        override fun fetchVector(vertexIndex: Long): FloatArray {
            val start = (vertexIndex * vectorDim).toInt()
            val end = start + vectorDim

            return vectors.copyOfRange(start, end)
        }

        override fun fetchNeighbours(vertexIndex: Long): LongArray {
            var initVersion = edgeVersions[vertexIndex.toInt()]
            while (true) {
                val edgesOffset = (vertexIndex * (maxConnectionsPerVertex + 1)).toInt()
                val size = edges[edgesOffset].toInt()

                assert(size in 0..maxConnectionsPerVertex)
                val result = edges.copyOfRange(edgesOffset + 1, edgesOffset + 1 + size)
                val finalVersion = edgeVersions[vertexIndex.toInt()]
                if (validateVersion(finalVersion, initVersion)) {
                    return result
                }
                initVersion = finalVersion
            }
        }

        fun setNeighbours(vertexIndex: Long, neighbours: LongArray, size: Int = neighbours.size) {
            edgeVersions.incrementAndGet(vertexIndex.toInt())
            assert(size in 0..maxConnectionsPerVertex)

            val edgesOffset = (vertexIndex * (maxConnectionsPerVertex + 1)).toInt()

            edges[edgesOffset] = size.toLong()
            neighbours.copyInto(edges, edgesOffset + 1, 0, size)
            edgeVersions.incrementAndGet(vertexIndex.toInt())
        }

        fun appendNeighbour(vertexIndex: Long, neighbour: Long) {
            edgeVersions.incrementAndGet(vertexIndex.toInt())
            val edgesOffset = (vertexIndex * (maxConnectionsPerVertex + 1)).toInt()
            val size = edges[edgesOffset].toInt()

            edges[edgesOffset] = (size + 1).toLong()
            edges[edgesOffset + size + 1] = neighbour
            edgeVersions.incrementAndGet(vertexIndex.toInt())
        }

        fun generateRandomEdges() {
            if (size == 1) {
                return
            }

            val rng = RandomSource.XO_RO_SHI_RO_128_PP.create()
            val shuffledIndexes = PermutationSampler.natural(size)
            PermutationSampler.shuffle(rng, shuffledIndexes)

            val maxEdges = min(size - 1, maxConnectionsPerVertex)
            var shuffleIndex = 0
            for (i in 0 until size) {
                var edgesOffset = i * (maxConnectionsPerVertex + 1)
                edges[edgesOffset] = maxEdges.toLong()

                var addedEdges = 0
                while (addedEdges < maxEdges) {
                    val randomIndex = shuffledIndexes[shuffleIndex].toLong()
                    shuffleIndex++

                    if (shuffleIndex == size) {
                        PermutationSampler.shuffle(rng, shuffledIndexes)
                        shuffleIndex = 0
                    } else if (randomIndex == i.toLong()) {
                        continue
                    }

                    edges[++edgesOffset] = randomIndex
                    addedEdges++
                }
            }
        }

        fun getNeighboursAndClear(vertexIndex: Long): LongArray {
            val edgesOffset = (vertexIndex * (maxConnectionsPerVertex + 1)).toInt()
            val result = fetchNeighbours(vertexIndex)

            edgeVersions.incrementAndGet(vertexIndex.toInt())
            edges[edgesOffset] = 0L
            edgeVersions.incrementAndGet(vertexIndex.toInt())

            return result
        }

        override fun medoid(): Long {
            if (medoid == -1L) {
                medoid = calculateMedoid()
            }

            return medoid
        }

        private fun calculateMedoid(): Long {
            if (size == 1) {
                return 0
            }

            val meanVector = FloatArray(vectorDim)

            for (i in 0 until size) {
                val vector = fetchVector(i.toLong())
                for (j in 0 until vectorDim) {
                    meanVector[j] += vector[j]
                }
            }

            for (j in 0 until vectorDim) {
                meanVector[j] = meanVector[j] / size
            }

            var minDistance = Double.POSITIVE_INFINITY
            var medoidIndex = -1

            for (i in 0 until size) {
                val distance = distanceFunction.computeDistance(
                    vectors,
                    i * vectorDim, meanVector, 0, vectorDim
                )

                if (distance < minDistance) {
                    minDistance = distance
                    medoidIndex = i
                }
            }

            return medoidIndex.toLong()
        }


        fun saveToDisk() {
            val verticesPerPage = pageSize / vertexRecordSize

            var wittenVertices = 0

            var vectorsOffset = 0
            var pageIndex = 0

            while (wittenVertices < size) {
                val page = ByteArray(pageSize)
                LONG_VIEW_VAR_HANDLE.set(page, 0, size)

                val verticesToWrite = minOf(verticesPerPage, size - wittenVertices)


                for (i in 0 until verticesToWrite) {
                    var edgesOffset = wittenVertices * (maxConnectionsPerVertex + 1)
                    var dataOffset = Long.SIZE_BYTES + i * vertexRecordSize

                    for (j in 0 until vectorDim) {
                        FLOAT_VIEW_VAR_HANDLE.set(page, dataOffset, vectors[vectorsOffset])
                        vectorsOffset++
                        dataOffset += Float.SIZE_BYTES
                    }

                    LONG_VIEW_VAR_HANDLE.set(page, dataOffset, ids[wittenVertices])
                    dataOffset += Long.SIZE_BYTES

                    val edgesSize = edges[edgesOffset]
                    assert(edgesSize in 0..maxConnectionsPerVertex)
                    edgesOffset++
                    page[dataOffset++] = edgesSize.toByte()

                    for (j in 0 until edgesSize) {
                        LONG_VIEW_VAR_HANDLE.set(page, dataOffset, edges[edgesOffset])
                        edgesOffset++
                        dataOffset += Long.SIZE_BYTES
                    }

                    wittenVertices++
                }

                graphPages[pageIndex.toLong()] = page
                pageIndex++
            }
        }
    }

    private inner class DiskGraph(val medoid: Long) : Graph {
        override fun medoid(): Long {
            return medoid
        }

        override fun fetchId(vertexIndex: Long): Long {
            if (vertexIndex >= verticesSize) {
                throw IllegalArgumentException()
            }

            val vertexPageIndex = vertexIndex / verticesPerPage
            val vertexOffset = (vertexIndex % verticesPerPage).toInt() * vertexRecordSize + Long.SIZE_BYTES

            val vertexPage = graphPages[vertexPageIndex]
            return LONG_VIEW_VAR_HANDLE.get(vertexPage, vertexOffset + vectorDim * Float.SIZE_BYTES) as Long
        }

        override fun fetchVector(vertexIndex: Long): FloatArray {
            if (vertexIndex >= verticesSize) {
                throw IllegalArgumentException()
            }

            val vertexPageIndex = vertexIndex / verticesPerPage
            val vertexPage = graphPages[vertexPageIndex]
            val vertexOffset = (vertexIndex % verticesPerPage).toInt() * vertexRecordSize + Long.SIZE_BYTES

            return fetchVectorData(vertexPage, vertexOffset)
        }

        private fun fetchVectorData(page: ByteArray, offset: Int): FloatArray {
            val vectorData = FloatArray(vectorDim)

            for (i in 0 until vectorDim) {
                vectorData[i] =
                    FLOAT_VIEW_VAR_HANDLE.get(page, offset + i * Float.SIZE_BYTES) as Float
            }

            return vectorData
        }

        override fun fetchNeighbours(vertexIndex: Long): LongArray {
            if (vertexIndex >= verticesSize) {
                throw IllegalArgumentException()
            }

            val vertexPageIndex = vertexIndex / verticesPerPage
            val vertexPage = graphPages[vertexPageIndex]

            val vertexOffset =
                (vertexIndex % verticesPerPage).toInt() * vertexRecordSize +
                        vectorDim * Float.SIZE_BYTES + Long.SIZE_BYTES + 1 + Long.SIZE_BYTES


            val neighboursSize = java.lang.Byte.toUnsignedInt(vertexPage[vertexOffset - 1])
            assert(neighboursSize <= maxConnectionsPerVertex)
            assert(neighboursSize >= 0)

            val result = LongArray(neighboursSize)
            for (i in 0 until neighboursSize) {
                result[i] =
                    LONG_VIEW_VAR_HANDLE.get(vertexPage, vertexOffset + i * Long.SIZE_BYTES) as Long
            }

            return result
        }

    }
}



