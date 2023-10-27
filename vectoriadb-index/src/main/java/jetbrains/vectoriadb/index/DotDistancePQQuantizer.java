/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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


import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

@SuppressWarnings("unused")
public final class DotDistancePQQuantizer { //}extends AbstractQuantizer {
//    private int quantizersCount;
//
//    private L2UnitQuantizer l2UnitQuantizer;
//
//    private float[][][] centroids;
//
//
//    @Override
//    MemorySegment allocateMemoryForPqVectors(int quantizersCount, int vectorsCount, Arena arena) {
//        throw new UnsupportedOperationException();
//    }
//
//    @Override
//    public void generatePQCodes(final int vectorsDimension, final int compressionRatio, final VectorReader vectorReader) {
//        l2UnitQuantizer = new L2UnitQuantizer();
//
//        var vectorNormalizingReader = new VectorNormalizingReader(vectorReader);
//        l2UnitQuantizer.generatePQCodes(vectorsDimension, compressionRatio, vectorNormalizingReader);
//
//        var vectorsCount = vectorReader.size();
//        var pqVectors = l2UnitQuantizer.encodedVectors();
//
//
//        for (int i = 0; i < vectorsCount; i++) {
//            var vector = vectorReader.read(i);
//            var norm = -DotDistanceFunction.INSTANCE.computeDistance(vector, 0,
//                    vector, 0, vectorsDimension);
//        }
//    }
//
//    @Override
//    public void close() {
//        l2UnitQuantizer.close();
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
