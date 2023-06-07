package jetbrains.exodus.diskann

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.longs.LongArrayList
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import mu.KLogging
import org.apache.commons.rng.sampling.PermutationSampler
import org.apache.commons.rng.simple.RandomSource
import java.lang.invoke.MethodHandles
import java.lang.invoke.VarHandle
import java.nio.ByteOrder
import java.util.*
import java.util.concurrent.ThreadLocalRandom
import kotlin.math.min

const val PAGE_SIZE_MULTIPLIER = 4 * 1024

internal class DiskANN(
    private val vectorDim: Int, private val distanceFunction: DistanceFunction,
    private val distanceMultiplication: Float = 2.0f, private val maxConnectionsPerVertex: Int = 70,
    private val maxAmountOfCandidates: Int = 125,
    private val kMeansIterationThreshold: Int = 1_000
) {
    private var verticesSize: Long = 0
    private val graphPages: Long2ObjectOpenHashMap<ByteArray> = Long2ObjectOpenHashMap()

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

    private fun greedySearch(
        graph: Graph,
        startVertexIndex: Long,
        queryVertex: FloatArray,
        maxResultSize: Int
    ): Pair<LongArray, LongOpenHashSet> {
        val nearestCandidates = TreeSet<GreedyVertex>()

        val visitedVertexIndices = LongOpenHashSet(maxAmountOfCandidates)
        val visitedVertices = ArrayList<GreedyVertex>(maxAmountOfCandidates)

        val startVector = graph.fetchVector(startVertexIndex)

        val startVertex = GreedyVertex(
            startVertexIndex, distanceFunction.computeDistance(queryVertex, startVector)
        )

        nearestCandidates.add(startVertex)
        while (nearestCandidates.isNotEmpty()) {
            val minVertex = nearestCandidates.pollFirst()!!
            visitedVertexIndices.add(minVertex.index)
            visitedVertices.add(minVertex)

            val vertexNeighbours = graph.fetchNeighbours(minVertex.index)
            for (vertexIndex in vertexNeighbours) {
                if (visitedVertexIndices.contains(vertexIndex)) {
                    continue
                }

                val vertexVector = graph.fetchVector(vertexIndex)
                val vertex =
                    GreedyVertex(vertexIndex, distanceFunction.computeDistance(queryVertex, vertexVector))
                nearestCandidates.add(vertex)
            }

            break
        }

        val resultSize = min(maxResultSize, nearestCandidates.size + visitedVertexIndices.size)
        val nearestVertices = LongArray(resultSize)

        val nearestCandidateIterator = nearestCandidates.iterator()
        visitedVertices.sort()

        val commonSize = min(nearestCandidates.size, visitedVertices.size)

        var visitedIndex = 0
        var currentSize = 0
        for (i in 0 until commonSize) {
            val nearestCandidate = nearestCandidateIterator.next()
            val visitedVertex = visitedVertices[visitedIndex]

            if (nearestCandidate.distance < visitedVertex.distance) {
                nearestVertices[i] = nearestCandidate.index
            } else {
                nearestVertices[i] = visitedVertex.index
                visitedIndex++
            }

            currentSize++
        }

        if (currentSize < resultSize) {
            if (visitedIndex < visitedVertices.size) {
                for (i in currentSize until nearestVertices.size) {
                    nearestVertices[i] = visitedVertices[visitedIndex].index
                    visitedIndex++
                }
            } else {
                for (i in currentSize until nearestVertices.size) {
                    nearestVertices[i] = nearestCandidateIterator.next().index
                }
            }
        }


        return Pair(nearestVertices, visitedVertexIndices)
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

        while (cachedCandidates.isNotEmpty()) {
            val min = cachedCandidates.pollFirst()!!
            neighbours.add(min.index)

            val iterator = cachedCandidates.iterator()
            while (iterator.hasNext()) {
                val candidate = iterator.next()
                val distance = distanceFunction.computeDistance(min.vector, candidate.vector)

                if (distance * distanceMultiplication <= candidate.distance) {
                    iterator.remove()
                }
            }
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

        for (i in permutation) {
            val (_, visited) = greedySearch(graph, medoid, graph.fetchVector(i.toLong()), maxAmountOfCandidates)
            robustPrune(graph, i.toLong(), visited, 1.0f)

            val neighbours = graph.fetchNeighbours(i.toLong())
            for (neighbour in neighbours) {
                val neighbourNeighbours = graph.fetchNeighbours(neighbour)

                if (!neighbourNeighbours.contains(i.toLong())) {
                    if (neighbourNeighbours.size + 1 <= maxConnectionsPerVertex) {
                        graph.appendNeighbour(neighbour, i.toLong())
                    }
                } else {
                    robustPrune(graph, neighbour, LongOpenHashSet(longArrayOf(i.toLong())), distanceMultiplication)
                }
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

        private var medoid: Long = -1

        fun addVector(id: Long, vector: FloatArray) {
            val offset = size * vectorDim
            vector.copyInto(vectors, offset, 0, vectorDim)

            ids[size] = id
            size++

            medoid = -1
        }

        fun getNeighboursSize(vertexIndex: Long) = edges[(vertexIndex * (maxConnectionsPerVertex + 1)).toInt()].toInt()
        override fun fetchId(vertexIndex: Long): Long {
            return ids[vertexIndex.toInt()]
        }

        override fun fetchVector(vertexIndex: Long): FloatArray {
            val start = (vertexIndex * vectorDim).toInt()
            val end = start + vectorDim

            return vectors.copyOfRange(start, end)
        }

        override fun fetchNeighbours(vertexIndex: Long): LongArray {
            val edgesOffset = (vertexIndex * (maxConnectionsPerVertex + 1)).toInt()
            val size = edges[edgesOffset].toInt()

            return edges.copyOfRange(edgesOffset + 1, edgesOffset + 1 + size)
        }

        fun setNeighbours(vertexIndex: Long, neighbours: LongArray, size: Int = neighbours.size) {
            val edgesOffset = (vertexIndex * (maxConnectionsPerVertex + 1)).toInt()

            edges[edgesOffset] = size.toLong()
            neighbours.copyInto(edges, edgesOffset + 1, 0, size)
        }

        fun appendNeighbour(vertexIndex: Long, neighbour: Long) {
            val edgesOffset = (vertexIndex * (maxConnectionsPerVertex + 1)).toInt()
            val size = edges[edgesOffset].toInt()

            edges[edgesOffset] = (size + 1).toLong()
            edges[edgesOffset + size + 1] = neighbour
        }

        fun generateRandomEdges() {
            val rnd = ThreadLocalRandom.current()
            if (size == 1) {
                return
            }

            for (i in 0 until size) {
                var edgesOffset = i * (maxConnectionsPerVertex + 1)

                edges[edgesOffset] = maxConnectionsPerVertex.toLong()

                val maxEdges = min(size - 1, maxConnectionsPerVertex)
                val visited = LongOpenHashSet(maxEdges)

                var addedEdges = 0
                while (addedEdges < maxEdges) {
                    var randomIndex = rnd.nextInt(size).toLong()

                    if (randomIndex == i.toLong()) {
                        randomIndex = (randomIndex + 1) % size
                    }

                    if (visited.add(randomIndex)) {
                        edges[++edgesOffset] = randomIndex
                        addedEdges++
                    }
                }
            }
        }

        fun getNeighboursAndClear(vertexIndex: Long): LongArray {
            val edgesOffset = (vertexIndex * (maxConnectionsPerVertex + 1)).toInt()
            val result = fetchNeighbours(vertexIndex)

            edges[edgesOffset] = 0L

            return result
        }

        override fun medoid(): Long {
            if (medoid == -1L) {
                medoid = calculateMedoid()
            }

            return medoid
        }

        private fun calculateMedoid(): Long {
            val rng = RandomSource.XO_RO_SHI_RO_128_PP.create(32674)

            var prevMedoidIndex = -1
            var medoidIndex = rng.nextInt(size)
            val weightedDistances = DoubleArray(size)

            var iterations = 0

            while (prevMedoidIndex != medoidIndex && iterations < kMeansIterationThreshold) {
                prevMedoidIndex = medoidIndex
                val medoidOffset = medoidIndex * vectorDim

                var distanceSum = 0.0
                for (i in 0 until size) {
                    val vectorOffset = i * vectorDim
                    val distance = distanceFunction.computeDistance(vectors, medoidOffset, vectors, vectorOffset)
                    val weightedDistance = distance * distance

                    distanceSum += weightedDistance
                    weightedDistances[i] = weightedDistance
                }

                for (i in 0 until size) {
                    weightedDistances[i] /= distanceSum
                }

                for (i in 1 until size - 1) {
                    weightedDistances[i] += weightedDistances[i - 1]
                }

                weightedDistances[size - 1] = 1.0

                val randomValue = rng.nextDouble()
                medoidIndex = Arrays.binarySearch(weightedDistances, randomValue)

                if (medoidIndex < 0) {
                    medoidIndex = -medoidIndex - 1
                }

                assert(medoidIndex in 0 until size)

                iterations++
            }

            return medoidIndex.toLong()
        }


        fun saveToDisk() {
            val verticesPerPage = pageSize / vertexRecordSize

            var wittenVertices = 0

            var edgesOffset = 0
            var vectorsOffset = 0
            var pageIndex = 0

            while (wittenVertices < size) {
                val page = ByteArray(pageSize)
                LONG_VIEW_VAR_HANDLE.set(page, 0, size)

                val verticesToWrite = minOf(verticesPerPage, size - wittenVertices)
                var dataOffset = Long.SIZE_BYTES

                for (i in 0 until verticesToWrite) {
                    for (j in 0 until vectorDim) {
                        FLOAT_VIEW_VAR_HANDLE.set(page, dataOffset, vectors[vectorsOffset++])
                        dataOffset += Float.SIZE_BYTES
                    }

                    val edgesSize = edges[edgesOffset++]
                    page[dataOffset++] = edgesSize.toByte()

                    for (j in 0 until edgesSize) {
                        LONG_VIEW_VAR_HANDLE.set(page, dataOffset, edges[edgesOffset++])
                        dataOffset += Long.SIZE_BYTES
                    }
                }

                wittenVertices += verticesToWrite
                graphPages[(pageIndex++).toLong()] = page
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
            val vertexOffset = (vertexPageIndex % verticesPerPage).toInt() * vertexRecordSize + Long.SIZE_BYTES

            val vertexPage = graphPages[vertexPageIndex]
            return LONG_VIEW_VAR_HANDLE.get(vertexPage, vertexOffset + vectorDim * Float.SIZE_BYTES) as Long
        }

        override fun fetchVector(vertexIndex: Long): FloatArray {
            if (vertexIndex >= verticesSize) {
                throw IllegalArgumentException()
            }

            val vertexPageIndex = vertexIndex / verticesPerPage
            val vertexPage = graphPages[vertexPageIndex]
            val vertexOffset = (vertexPageIndex % verticesPerPage).toInt() * vertexRecordSize + Long.SIZE_BYTES

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
                (vertexPageIndex % verticesPerPage).toInt() * vertexRecordSize +
                        vectorDim * Float.SIZE_BYTES + Long.SIZE_BYTES + 1 + Long.SIZE_BYTES


            val neighboursSize = vertexPage[vertexOffset - 1].toInt()
            assert(neighboursSize <= maxConnectionsPerVertex)

            val result = LongArray(neighboursSize)
            for (i in 0 until neighboursSize) {
                result[i] =
                    LONG_VIEW_VAR_HANDLE.get(vertexPage, vertexOffset + i * Long.SIZE_BYTES) as Long
            }

            return result
        }

    }
}



