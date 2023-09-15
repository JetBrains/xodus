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


@SuppressWarnings("unused")
public final class DotDistancePQQuantizer {
//
//    @Override
//    public Parameters calculatePQParameters(int vectorDim, int compression) {
//        var pqSubVectorSize = compression / Float.BYTES;
//        var quantizersCount = vectorDim / pqSubVectorSize;
//
//        if (compression % Float.BYTES != 0) {
//            throw new IllegalArgumentException(
//                    "Vector should be divided during creation of PQ codes without remainder.");
//        }
//
//        if (vectorDim % pqSubVectorSize != 0) {
//            throw new IllegalArgumentException(
//                    "Vector should be divided during creation of PQ codes without remainder.");
//        }
//
//        return new Parameters(pqSubVectorSize, quantizersCount + 1);
//    }
//
//    @Override
//    public Codes generatePQCodes(int quantizersCount, int subVectorSize, VectorReader vectorReader, Arena arena) {
//        var l2PQuantizer = new L2UnitQuantizer();
//
//        l2PQuantizer.generatePQCodes(quantizersCount - 1, subVectorSize,
//                new VectorNormalizingReader(vectorReader), arena);
//
//
//        return null;
//    }
//
//    @Override
//    public void buildDistanceLookupTable(float[] vector, float[] lookupTable, float[][][] centroids,
//                                         int quantizersCount, int subVectorSize, DistanceFunction distanceFunction) {
//
//    }
//
//    @Override
//    public float computeDistance(MemorySegment vectors, float[] lookupTable, int vectorIndex, int quantizersCount) {
//
//        return 0;
//    }
//
//    private static final class L2UnitQuantizer extends L2PQQuantizer {
//        private MemorySegment pqVectors;
//
//        @Override
//        public MemorySegment allocateMemoryForPqVectors(int quantizersCount, int vectorsCount, Arena arena) {
//            var allocationSize = (long) vectorsCount * (quantizersCount + 1);
//            pqVectors = arena.allocate(allocationSize);
//
//            return pqVectors.asSlice(quantizersCount);
//        }
//    }
//
//    private static class VectorNormalizingReader implements VectorReader {
//        private final VectorReader vectorReader;
//
//        private VectorNormalizingReader(VectorReader vectorReader) {
//            this.vectorReader = vectorReader;
//        }
//
//        @Override
//        public MemorySegment read(int index) {
//            var vector = vectorReader.read(index);
//            var vectorSize = (int) (vector.byteSize() / Float.BYTES);
//
//            var norm = -DotDistanceFunction.INSTANCE.computeDistance(vector, 0,
//                    vector, 0, vectorSize);
//            var heapSegment = MemorySegment.ofArray(new byte[(int) vector.byteSize()]);
//
//            for (int i = 0; i < vectorSize; i++) {
//                var value = vector.getAtIndex(ValueLayout.JAVA_FLOAT, i);
//                heapSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, value / norm);
//            }
//
//            return heapSegment;
//        }
//
//        @Override
//        public int size() {
//            return vectorReader.size();
//        }
//
//        @Override
//        public void close() throws Exception {
//            vectorReader.close();
//        }
//    }
}
