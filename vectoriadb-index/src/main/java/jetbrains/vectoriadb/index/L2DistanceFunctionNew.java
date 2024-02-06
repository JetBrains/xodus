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

import java.lang.foreign.MemorySegment;

public final class L2DistanceFunctionNew implements DistanceFunction {

    @Override
    public float computeDistance(float[] firstVector, int firstVectorFrom, float[] secondVector, int secondVectorFrom, int size) {
        return VectorOperations.l2Distance(firstVector, firstVectorFrom, secondVector, secondVectorFrom, size);
    }

    @Override
    public float computeDistance(MemorySegment firstSegment, long firstSegmentFromOffset, float[] secondVector, int secondVectorOffset, int size) {
        return VectorOperations.l2Distance(firstSegment, firstSegmentFromOffset / Float.BYTES, secondVector, secondVectorOffset, size);
    }

    @Override
    public float computeDistance(MemorySegment firstSegment, long firstSegmentOffset, MemorySegment secondSegment, long secondSegmentOffset, int size) {
        return VectorOperations.l2Distance(firstSegment, firstSegmentOffset / Float.BYTES, secondSegment, secondSegmentOffset / Float.BYTES, size);
    }

    @Override
    public void computeDistance(MemorySegment originSegment, long originSegmentOffset, MemorySegment firstSegment, long firstSegmentOffset, MemorySegment secondSegment, long secondSegmentOffset, MemorySegment thirdSegment, long thirdSegmentOffset, MemorySegment fourthSegment, long fourthSegmentOffset, int size, float[] result) {
        VectorOperations.l2DistanceBatch(
                originSegment, originSegmentOffset / Float.BYTES,
                firstSegment, firstSegmentOffset / Float.BYTES,
                secondSegment, secondSegmentOffset / Float.BYTES,
                thirdSegment, thirdSegmentOffset / Float.BYTES,
                fourthSegment, fourthSegmentOffset / Float.BYTES,
                size, result
        );
    }

    @Override
    public void computeDistance(float[] originVector, int originVectorOffset, MemorySegment firstSegment, long firstSegmentOffset, MemorySegment secondSegment, long secondSegmentOffset, MemorySegment thirdSegment, long thirdSegmentOffset, MemorySegment fourthSegment, long fourthSegmentOffset, int size, float[] result) {
        VectorOperations.l2DistanceBatch(
                originVector, originVectorOffset,
                firstSegment, firstSegmentOffset / Float.BYTES,
                secondSegment, secondSegmentOffset / Float.BYTES,
                thirdSegment, thirdSegmentOffset / Float.BYTES,
                fourthSegment, fourthSegmentOffset / Float.BYTES,
                size, result
        );
    }

    @Override
    public void computeDistance(float[] originVector, int originVectorOffset, float[] firstVector, int firstVectorOffset, float[] secondVector, int secondVectorOffset, float[] thirdVector, int thirdVectorOffset, float[] fourthVector, int fourthVectorOffset, float[] result, int size) {
        VectorOperations.l2DistanceBatch(
                originVector, originVectorOffset,
                firstVector, firstVectorOffset,
                secondVector, secondVectorOffset,
                thirdVector, thirdVectorOffset,
                fourthVector, fourthVectorOffset,
                size, result
        );
    }

    @Override
    public float computeDistance(float scalar1, float scalar2) {
        var tmp = (scalar1 - scalar2);
        return tmp * tmp;
    }
}
