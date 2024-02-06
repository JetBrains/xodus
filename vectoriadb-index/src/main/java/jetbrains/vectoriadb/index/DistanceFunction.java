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

import jdk.incubator.vector.FloatVector;

import java.lang.foreign.MemorySegment;

public interface DistanceFunction {
    int PREFERRED_SPECIES_LENGTH = FloatVector.SPECIES_PREFERRED.length();

    float computeDistance(float[] firstVector, int firstVectorFrom,
                          float[] secondVector, int secondVectorFrom,
                          int size);

    float computeDistance(MemorySegment firstSegment, long firstSegmentFromOffset,
                          float[] secondVector, int secondVectorOffset,
                          int size);

    float computeDistance(MemorySegment firstSegment, long firstSegmentOffset,
                          MemorySegment secondSegment, long secondSegmentOffset,
                          int size);

    void computeDistance(MemorySegment originSegment, long originSegmentOffset,
                         MemorySegment firstSegment, long firstSegmentOffset,
                         MemorySegment secondSegment, long secondSegmentOffset,
                         MemorySegment thirdSegment, long thirdSegmentOffset,
                         MemorySegment fourthSegment, long fourthSegmentOffset,
                         int size, float[] result);

    void computeDistance(float[] originVector, @SuppressWarnings("SameParameterValue") int originVectorOffset,
                         MemorySegment firstSegment, long firstSegmentFromOffset,
                         MemorySegment secondSegment, long secondSegmentFromOffset,
                         MemorySegment thirdSegment, long thirdSegmentFromOffset,
                         MemorySegment fourthSegment, long fourthSegmentFromOffset,
                         int size, float[] result);

    void computeDistance(float[] originVector, @SuppressWarnings("SameParameterValue") int originVectorOffset,
                         float[] firstVector, int firstVectorOffset,
                         float[] secondVector, int secondVectorOffset,
                         float[] thirdVector, int thirdVectorOffset,
                         float[] fourthVector, int fourthVectorOffset,
                         final float[] result, int size);

    float computeDistance(float scalar1, float scalar2);

    default float[] preProcess(float[] vector, float[] result) {
        return vector;
    }

    default int findClosestVector(float[] vectors, MemorySegment vector, int from, int size) {
        var minDistance = Float.MAX_VALUE;
        var minIndex = -1;

        var numVectors = vectors.length / size;
        var vectorFrom = from * Float.BYTES;

        for (int i = 0; i < numVectors; i++) {
            var distance = computeDistance(vector, vectorFrom, vectors,
                    i * size, size);

            if (distance < minDistance) {
                minDistance = distance;
                minIndex = i;
            }
        }

        return minIndex;
    }


    default void findClosestVector(float[] vectors, MemorySegment vector1, MemorySegment vector2,
                                   MemorySegment vector3, MemorySegment vector4,
                                   int from, int size, int[] result) {
        var minDistance_1 = Float.MAX_VALUE;
        var minDistance_2 = Float.MAX_VALUE;
        var minDistance_3 = Float.MAX_VALUE;
        var minDistance_4 = Float.MAX_VALUE;

        var minIndex_1 = -1;
        var minIndex_2 = -1;
        var minIndex_3 = -1;
        var minIndex_4 = -1;

        var distance = new float[4];

        var numVectors = vectors.length / size;
        var vectorFrom = from * Float.BYTES;
        for (int i = 0; i < numVectors; i++) {
            computeDistance(vectors,
                    i * size,
                    vector1, vectorFrom,
                    vector2, vectorFrom,
                    vector3, vectorFrom,
                    vector4, vectorFrom, size,
                    distance);

            if (distance[0] < minDistance_1) {
                minDistance_1 = distance[0];
                minIndex_1 = i;
            }
            if (distance[1] < minDistance_2) {
                minDistance_2 = distance[1];
                minIndex_2 = i;
            }
            if (distance[2] < minDistance_3) {
                minDistance_3 = distance[2];
                minIndex_3 = i;
            }
            if (distance[3] < minDistance_4) {
                minDistance_4 = distance[3];
                minIndex_4 = i;
            }
        }

        result[0] = minIndex_1;
        result[1] = minIndex_2;
        result[2] = minIndex_3;
        result[3] = minIndex_4;
    }

    static int closestSIMDStep(int step, int size) {
        return Integer.highestOneBit(Math.min(step, size));
    }
}
