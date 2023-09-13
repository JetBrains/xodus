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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import jdk.incubator.vector.FloatVector;
import jetbrains.vectoriadb.index.diskcache.DiskCache;
import jetbrains.vectoriadb.index.util.collections.BoundedGreedyVertexPriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class IndexReader implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(IndexReader.class);
    private final int medoid;
    private final DiskCache diskCache;
    private final int vectorDim;
    //1st dimension quantizer index
    //2nd index of code inside code book
    //3d dimension centroid vector
    private final float[][][] pqCentroids;
    private final MemorySegment pqVectors;
    private final ThreadLocal<NearestGreedySearchCachedData> nearestGreedySearchCachedDataThreadLocal;
    private final DistanceFunction distanceFunction;
    private final Quantizer quantizer;
    private final int maxAmountOfCandidates;
    private final Arena arena = Arena.openShared();
    private final int pqSubVectorSize;
    private final int pqQuantizersCount;
    private long pqReCalculated = 0;
    private double pqReCalculationError = 0.0;
    private final Path path;
    private final String name;

    private volatile boolean closed;

    public IndexReader(String name, int vectorDim, Path indexDirPath, long cacheSize,
                       Quantizer quantizer, DistanceFunction distanceFunction) throws IOException {
        this(name, vectorDim, 64, 128, 32, indexDirPath,
                cacheSize, quantizer, distanceFunction);
    }

    public IndexReader(String name, int vectorDim, int maxConnectionsPerVertex, int maxAmountOfCandidates,
                       int pqCompression, Path indexDirPath, long cacheSize,
                       Quantizer quantizer, DistanceFunction distanceFunction) throws IOException {
        this.vectorDim = vectorDim;
        this.maxAmountOfCandidates = maxAmountOfCandidates;
        this.distanceFunction = distanceFunction;
        this.quantizer = quantizer;
        this.path = indexDirPath;
        this.name = name;

        var pageStructure = DiskCache.createPageStructure(vectorDim, maxConnectionsPerVertex);

        logger.info("IndexReader initialization : file block size {}, page size {}, vertex record size {}, " +
                        "vertices count per page {}",
                DiskCache.DISK_BLOCK_SIZE, pageStructure.pageSize(), pageStructure.vertexRecordSize(),
                pageStructure.verticesCountPerPage());
        logger.info("Vector index {} has been initialized. Vector lane count for distance calculation " +
                "is {}", name, FloatVector.SPECIES_PREFERRED.length());


        if (pqCompression % Float.BYTES != 0) {
            throw new IllegalArgumentException(
                    "Vector should be divided during creation of PQ codes without remainder.");
        }

        var pqFilePath = indexDirPath.resolve(name + ".data");
        var graphFilePath = indexDirPath.resolve(name + ".graph");
        try (var pqInputStream = Files.newInputStream(pqFilePath, StandardOpenOption.READ)) {
            try (var dataInputStream = new DataInputStream(pqInputStream)) {
                medoid = dataInputStream.readInt();
                dataInputStream.readInt();

                pqQuantizersCount = dataInputStream.readInt();
                int pqCodeBaseSize = dataInputStream.readInt();
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


        if (vectorDim % pqSubVectorSize != 0) {
            throw new IllegalArgumentException(
                    "Vector should be divided during creation of PQ codes without remainder.");
        }

        logger.info("PQ quantizers count is " + pqQuantizersCount + ", sub vector size is " + pqSubVectorSize +
                " elements , compression is " + pqCompression + " for index '" + name + "'");

        nearestGreedySearchCachedDataThreadLocal = ThreadLocal.withInitial(() -> new NearestGreedySearchCachedData(
                new IntOpenHashSet(8 * 1024,
                        Hash.VERY_FAST_LOAD_FACTOR), new float[pqQuantizersCount * (1 << Byte.SIZE)],
                new BoundedGreedyVertexPriorityQueue(maxAmountOfCandidates), new int[maxConnectionsPerVertex],
                new int[maxAmountOfCandidates]));


        logger.info("Index data were loaded from disk for database {}", name);
        this.diskCache = new DiskCache(cacheSize, vectorDim, maxConnectionsPerVertex, graphFilePath);
    }

    public void nearest(float[] vector, long[] result, int resultSize) {
        if (closed) {
            throw new IllegalStateException("Index is closed");
        }

        var threadLocalCache = nearestGreedySearchCachedDataThreadLocal.get();

        var visitedVertexIndices = threadLocalCache.visistedVertexIndices;
        visitedVertexIndices.clear();

        var nearestCandidates = threadLocalCache.nearestCandidates;
        nearestCandidates.clear();

        var startVertexIndex = medoid;
        var vertexNeighbours = threadLocalCache.vertexNeighbours;

        var distanceResult = threadLocalCache.distanceResult;
        var vertexIndexesToCheck = threadLocalCache.vertexIndexesToCheck;
        vertexIndexesToCheck.clear();

        var vertexToPreload = threadLocalCache.vertexToPreload;

        var startVertexInMemoryPageIndex = diskCache.readLock(startVertexIndex);
        var startVectorOffset = diskCache.vectorOffset(startVertexInMemoryPageIndex, startVertexIndex);
        nearestCandidates.add(startVertexIndex,
                distanceFunction.computeDistance(diskCache.pages, startVectorOffset, vector, 0,
                        vectorDim),
                false, true);

        assert nearestCandidates.size() <= maxAmountOfCandidates;
        visitedVertexIndices.add(startVertexIndex);

        float[] lookupTable = null;

        while (true) {
            int currentVertex = -1;

            vertexRecalculationLoop:
            while (true) {
                vertexIndexesToCheck.clear();

                while (vertexIndexesToCheck.size() < 4) {
                    if (vertexIndexesToCheck.isEmpty()) {
                        preloadVertices(nearestCandidates, vertexToPreload);
                    }

                    var notCheckedVertex = nearestCandidates.nextNotCheckedVertexIndex();
                    if (notCheckedVertex < 0) {
                        if (vertexIndexesToCheck.isEmpty()) {
                            break vertexRecalculationLoop;
                        }

                        assert vertexIndexesToCheck.size() <= 4;
                        recalculateDistances(vector, nearestCandidates,
                                vertexIndexesToCheck, distanceResult);
                        continue;
                    }


                    if (nearestCandidates.isPqDistance(notCheckedVertex)) {
                        vertexIndexesToCheck.add(notCheckedVertex);
                        assert vertexIndexesToCheck.size() <= 4;
                    } else {
                        if (!vertexIndexesToCheck.isEmpty()) {
                            assert vertexIndexesToCheck.size() <= 4;
                            recalculateDistances(vector, nearestCandidates,
                                    vertexIndexesToCheck, distanceResult);
                            continue;
                        }

                        currentVertex = nearestCandidates.vertexIndex(notCheckedVertex);
                        nearestCandidates.markUnlocked(notCheckedVertex);

                        break vertexRecalculationLoop;
                    }
                }

                assert vertexIndexesToCheck.size() == 4;
                recalculateDistances(vector, nearestCandidates,
                        vertexIndexesToCheck, distanceResult);
            }

            if (currentVertex < 0) {
                break;
            }


            var edgesCount = diskCache.fetchEdges(currentVertex, vertexNeighbours);
            assert vertexIndexesToCheck.isEmpty();

            for (var i = 0; i < edgesCount; i++) {
                var vertexIndex = vertexNeighbours[i];

                if (visitedVertexIndices.add(vertexIndex)) {
                    if (lookupTable == null) {
                        lookupTable = threadLocalCache.lookupTable;
                        quantizer.buildDistanceLookupTable(vector, lookupTable, pqCentroids, pqQuantizersCount,
                                pqSubVectorSize, distanceFunction);
                    }

                    assert vertexIndexesToCheck.size() <= 4;

                    vertexIndexesToCheck.add(vertexIndex);
                    if (vertexIndexesToCheck.size() == 4) {
                        computePQDistances(lookupTable, vertexIndexesToCheck, nearestCandidates,
                                distanceResult);
                    }

                    assert vertexIndexesToCheck.size() <= 4;
                }
            }

            assert vertexIndexesToCheck.size() <= 4;

            if (!vertexIndexesToCheck.isEmpty()) {
                computePQDistances(lookupTable, vertexIndexesToCheck, nearestCandidates,
                        distanceResult);
            }

            assert vertexIndexesToCheck.isEmpty();
            assert nearestCandidates.size() <= maxAmountOfCandidates;

            diskCache.unlock(currentVertex);
        }

        var unlockSize = nearestCandidates.fetchAllLocked(vertexToPreload);
        for (int i = 0; i < unlockSize; i++) {
            diskCache.unlock(vertexToPreload[i]);
        }

        nearestCandidates.vertexIndices(result, resultSize);
    }

    private void preloadVertices(BoundedGreedyVertexPriorityQueue nearestCandidates, int[] vertexToPreload) {
        var preLoadSize = nearestCandidates.markAsLocked(8, vertexToPreload);

        for (int n = 0; n < preLoadSize; n++) {
            var preLoadVertexIndex = vertexToPreload[n];
            diskCache.preloadIfNeeded(preLoadVertexIndex);
        }
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
                                    float[] distanceResult) {
        assert distanceResult.length == 4;
        assert vertexIndexesToCheck.size() <= 4;

        var elements = vertexIndexesToCheck.elements();
        var size = vertexIndexesToCheck.size();

        if (size < 4) {
            for (int i = 0; i < size; i++) {
                var vertexIndex = elements[i];
                var pqDistance = quantizer.computeDistance(pqVectors, lookupTable, vertexIndex, pqQuantizersCount);

                addPqDistance(nearestCandidates, pqDistance, vertexIndex);
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
                addPqDistance(nearestCandidates, pqDistance, vertexIndex);
            }
        }

        vertexIndexesToCheck.clear();
    }

    private void addPqDistance(BoundedGreedyVertexPriorityQueue nearestCandidates, float pqDistance,
                               int vertexIndex) {
        if (nearestCandidates.size() < maxAmountOfCandidates) {
            var removed = nearestCandidates.add(vertexIndex, pqDistance, true, false);
            assert removed == Integer.MAX_VALUE;
        } else {
            var lastVertexDistance = nearestCandidates.maxDistance();

            if (lastVertexDistance >= pqDistance) {
                var removed = nearestCandidates.add(vertexIndex, pqDistance, true, false);
                assert removed != Integer.MAX_VALUE;

                //index is negative if it was not checked yet by the greedy search
                //all checked pages are unlocked in main cycle
                //but unchecked
                if (removed < 0) {
                    diskCache.unlock(-removed - 1);
                }
            }
        }
    }

    private void recalculateDistances(float[] queryVector, BoundedGreedyVertexPriorityQueue nearestCandidates,
                                      IntArrayList vertexIndexesToCheck, float[] distanceResult) {

        var elements = vertexIndexesToCheck.elements();
        var size = vertexIndexesToCheck.size();

        if (size < 4) {
            for (int i = 0; i < size; i++) {
                var notCheckedVertex = elements[i];

                var vertexIndex = nearestCandidates.vertexIndex(notCheckedVertex);
                if (nearestCandidates.isNotLockedForRead(notCheckedVertex)) {
                    throw new IllegalStateException("Vertex " + vertexIndex + " is not preloaded");
                }

                long inMemoryPageIndex = diskCache.readLocked(vertexIndex);
                var vectorOffset = diskCache.vectorOffset(inMemoryPageIndex, vertexIndex);

                var preciseDistance = distanceFunction.computeDistance(diskCache.pages, vectorOffset,
                        queryVector, 0, vectorDim);

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

            var pqDistance1 = nearestCandidates.vertexDistance(notCheckedVertex1);
            var pqDistance2 = nearestCandidates.vertexDistance(notCheckedVertex2);
            var pqDistance3 = nearestCandidates.vertexDistance(notCheckedVertex3);
            var pqDistance4 = nearestCandidates.vertexDistance(notCheckedVertex4);

            if (nearestCandidates.isNotLockedForRead(notCheckedVertex1)) {
                throw new IllegalStateException("Vertex " + vertexIndex1 + " is not preloaded");
            }
            if (nearestCandidates.isNotLockedForRead(notCheckedVertex2)) {
                throw new IllegalStateException("Vertex " + vertexIndex2 + " is not preloaded");
            }
            if (nearestCandidates.isNotLockedForRead(notCheckedVertex3)) {
                throw new IllegalStateException("Vertex " + vertexIndex3 + " is not preloaded");
            }
            if (nearestCandidates.isNotLockedForRead(notCheckedVertex4)) {
                throw new IllegalStateException("Vertex " + vertexIndex4 + " is not preloaded");
            }

            long inMemoryPageIndex1 = diskCache.readLocked(vertexIndex1);
            long inMemoryPageIndex2 = diskCache.readLocked(vertexIndex2);
            long inMemoryPageIndex3 = diskCache.readLocked(vertexIndex3);
            long inMemoryPageIndex4 = diskCache.readLocked(vertexIndex4);

            var vectorOffset1 = diskCache.vectorOffset(inMemoryPageIndex1, vertexIndex1);
            var vectorOffset2 = diskCache.vectorOffset(inMemoryPageIndex2, vertexIndex2);
            var vectorOffset3 = diskCache.vectorOffset(inMemoryPageIndex3, vertexIndex3);
            var vectorOffset4 = diskCache.vectorOffset(inMemoryPageIndex4, vertexIndex4);

            distanceFunction.computeDistance(queryVector, 0, diskCache.pages, vectorOffset1,
                    diskCache.pages, vectorOffset2, diskCache.pages, vectorOffset3,
                    diskCache.pages, vectorOffset4, vectorDim, distanceResult);


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

            notCheckedVertex3 =
                    notCheckedVertex3 - ((notCheckedVertex3 - newVertexIndex2 - 1) >>> (Integer.SIZE - 1));
            notCheckedVertex4 =
                    notCheckedVertex4 - ((notCheckedVertex4 - newVertexIndex2 - 1) >>> (Integer.SIZE - 1));
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
        }

        vertexIndexesToCheck.clear();
    }

    public long hits() {
        return diskCache.hits();
    }

    public void resetPQErrorStat() {
        pqReCalculated = 0;
        pqReCalculationError = 0.0;
    }

    public double pqErrorAvg() {
        return pqReCalculationError / pqReCalculated;
    }

    public void close() throws IOException {
        if (closed) {
            return;
        }

        closed = true;

        arena.close();
        diskCache.close();
    }

    public void deleteIndex() throws IOException {
        if (closed) {
            throw new IllegalStateException("Index is closed");
        }

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

    private static final class NearestGreedySearchCachedData {
        private final IntOpenHashSet visistedVertexIndices;
        private final float[] lookupTable;

        private final BoundedGreedyVertexPriorityQueue nearestCandidates;

        private final float[] distanceResult;

        private final IntArrayList vertexIndexesToCheck = new IntArrayList();

        private final int[] vertexNeighbours;
        private final int[] vertexToPreload;

        private NearestGreedySearchCachedData(IntOpenHashSet vertexIndices, float[] lookupTable,
                                              BoundedGreedyVertexPriorityQueue nearestCandidates,
                                              int[] vertexNeighbours, int[] vertexToPreload) {
            this.visistedVertexIndices = vertexIndices;
            this.lookupTable = lookupTable;
            this.nearestCandidates = nearestCandidates;
            this.vertexNeighbours = vertexNeighbours;
            this.vertexToPreload = vertexToPreload;
            this.distanceResult = new float[4];
        }
    }
}
