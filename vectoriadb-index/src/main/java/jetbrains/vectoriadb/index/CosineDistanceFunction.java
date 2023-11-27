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

public final class CosineDistanceFunction implements DistanceFunction {

    public static final CosineDistanceFunction INSTANCE = new CosineDistanceFunction();

    @Override
    public void computeDistance(MemorySegment originSegment, long originSegmentOffset, MemorySegment firstSegment,
                                long firstSegmentOffset, MemorySegment secondSegment, long secondSegmentOffset,
                                MemorySegment thirdSegment, long thirdSegmentOffset,
                                MemorySegment fourthSegment, long fourthSegmentOffset, int size, float[] result) {
        DotDistanceFunction.INSTANCE.computeDistance(originSegment, originSegmentOffset,
                firstSegment, firstSegmentOffset, secondSegment, secondSegmentOffset,
                thirdSegment, thirdSegmentOffset, fourthSegment, fourthSegmentOffset, size, result);
    }


    @Override
    public float computeDistance(MemorySegment firstSegment, long firstSegmentFromOffset,
                                 float[] secondVector, int secondVectorOffset, int size) {
        return DotDistanceFunction.INSTANCE.computeDistance(firstSegment, firstSegmentFromOffset,
                secondVector, secondVectorOffset, size);
    }

    @Override
    public void computeDistance(float[] originVector, int originVectorOffset,
                                MemorySegment firstSegment, long firstSegmentFromOffset,
                                MemorySegment secondSegment, long secondSegmentFromOffset,
                                MemorySegment thirdSegment, long thirdSegmentFromOffset,
                                MemorySegment fourthSegment, long fourthSegmentFromOffset, int size, float[] result) {
        DotDistanceFunction.INSTANCE.computeDistance(originVector, originVectorOffset,
                firstSegment, firstSegmentFromOffset, secondSegment, secondSegmentFromOffset,
                thirdSegment, thirdSegmentFromOffset, fourthSegment, fourthSegmentFromOffset, size, result);
    }

    @Override
    public float computeDistance(float[] firstVector, int firstVectorFrom, float[] secondVector, int secondVectorFrom, int size) {
        return DotDistanceFunction.INSTANCE.computeDistance(firstVector, firstVectorFrom, secondVector, secondVectorFrom, size);
    }

    @Override
    public void computeDistance(float[] originVector, int originVectorOffset, float[] firstVector, int firstVectorOffset,
                                float[] secondVector, int secondVectorOffset, float[] thirdVector, int thirdVectorOffset,
                                float[] fourthVector, int fourthVectorOffset, float[] result, int size) {
        DotDistanceFunction.INSTANCE.computeDistance(originVector, originVectorOffset, firstVector, firstVectorOffset,
                secondVector, secondVectorOffset, thirdVector, thirdVectorOffset, fourthVector, fourthVectorOffset,
                result, size);
    }

    @Override
    public float computeDistance(MemorySegment firstSegment, long firstSegmentOffset,
                                 MemorySegment secondSegment, long secondSegmentOffset, int size) {
        return DotDistanceFunction.INSTANCE.computeDistance(firstSegment, firstSegmentOffset,
                secondSegment, secondSegmentOffset, size);
    }

    @Override
    public float[] preProcess(float[] vector, float[] result) {
        VectorOperations.normalizeL2(vector, result);
        return result;
    }
}
