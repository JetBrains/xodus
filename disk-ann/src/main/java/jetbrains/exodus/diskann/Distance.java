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
package jetbrains.exodus.diskann;

import java.lang.foreign.MemorySegment;

public final class Distance {
    public static final byte L2_DISTANCE = 0;
    public static final byte DOT_DISTANCE = 1;

    public static float computeDistance(MemorySegment firstSegment, long firstSegmentFromOffset, MemorySegment secondSegment,
                                  long secondSegmentFromOffset, byte distanceFunction, int size) {
        if (distanceFunction == L2_DISTANCE) {
            return L2Distance.computeL2Distance(firstSegment, firstSegmentFromOffset, secondSegment, secondSegmentFromOffset,
                    size);
        } else if (distanceFunction == DOT_DISTANCE) {
            return DotDistance.computeDotDistance(firstSegment, firstSegmentFromOffset, secondSegment, secondSegmentFromOffset,
                    size);
        } else {
            throw new IllegalStateException("Unknown distance function: " + distanceFunction);
        }
    }

    public static void computeDistance(MemorySegment originSegment, long originSegmentOffset,
                                 MemorySegment firstSegment, long firstSegmentOffset,
                                 MemorySegment secondSegment, long secondSegmentOffset,
                                 MemorySegment thirdSegment, long thirdSegmentOffset,
                                 MemorySegment fourthSegment, long fourthSegmentOffset,
                                 byte distanceFunction,
                                 int size, float[] result) {
        if (distanceFunction == L2_DISTANCE) {
            L2Distance.computeL2Distance(originSegment, originSegmentOffset,
                    firstSegment, firstSegmentOffset, secondSegment, secondSegmentOffset,
                    thirdSegment, thirdSegmentOffset, fourthSegment, fourthSegmentOffset, size, result);

        } else if(distanceFunction == DOT_DISTANCE) {
            DotDistance.computeDotDistance(originSegment, originSegmentOffset,
                    firstSegment, firstSegmentOffset, secondSegment, secondSegmentOffset,
                    thirdSegment, thirdSegmentOffset, fourthSegment, fourthSegmentOffset, size, result);
        } else {
            throw new IllegalStateException("Unknown distance function: " + distanceFunction);
        }
    }

    public static float computeDistance(MemorySegment firstSegment, long firstSegmentFromOffset, float[] secondVector, byte distanceFunction) {
        if (distanceFunction == L2_DISTANCE) {
            return L2Distance.computeL2Distance(firstSegment, firstSegmentFromOffset, secondVector, 0);
        } else if (distanceFunction == DOT_DISTANCE) {
            return DotDistance.computeDotDistance(firstSegment, firstSegmentFromOffset, secondVector, 0);
        } else {
            throw new IllegalStateException("Unknown distance function: " + distanceFunction);
        }
    }

    public static void computeDistance(float[] originVector, @SuppressWarnings("SameParameterValue") int originVectorOffset,
                                 MemorySegment firstSegment,
                                 long firstSegmentFromOffset, MemorySegment secondSegment, long secondSegmentFromOffset,
                                 MemorySegment thirdSegment, long thirdSegmentFromOffset,
                                 MemorySegment fourthSegment, long fourthSegmentFromOffset,
                                 byte distanceFunction,
                                 int size,
                                 float[] result) {
        if (distanceFunction == L2_DISTANCE) {
            L2Distance.computeL2Distance(originVector, originVectorOffset, firstSegment, firstSegmentFromOffset,
                    secondSegment, secondSegmentFromOffset, thirdSegment, thirdSegmentFromOffset,
                    fourthSegment, fourthSegmentFromOffset, size, result);
        } else if (distanceFunction == DOT_DISTANCE) {
            DotDistance.computeDotDistance(originVector, originVectorOffset, firstSegment, firstSegmentFromOffset,
                    secondSegment, secondSegmentFromOffset, thirdSegment, thirdSegmentFromOffset,
                    fourthSegment, fourthSegmentFromOffset, size, result);
        } else {
            throw new IllegalStateException("Unknown distance function: " + distanceFunction);
        }
    }


    public static float computeDistance(final byte distanceFunction, float[] firstVector, float[] secondVector,
                                 int secondVectorFrom, int size) {
        if (distanceFunction == L2_DISTANCE) {
            return L2Distance.computeL2Distance(firstVector, 0, secondVector, secondVectorFrom, size);
        } else if (distanceFunction == DOT_DISTANCE) {
            return DotDistance.computeDotDistance(firstVector, 0, secondVector, secondVectorFrom, size);
        } else {
            throw new IllegalStateException("Unknown distance function: " + distanceFunction);
        }
    }

    public static void computeDistance(final byte distanceFunction, float[] originVector,
                                @SuppressWarnings("SameParameterValue") int originVectorOffset,
                                float[] firstVector, int firstVectorOffset,
                                float[] secondVector, int secondVectorOffset,
                                float[] thirdVector, int thirdVectorOffset,
                                float[] fourthVector, int fourthVectorOffset,
                                final float[] result,
                                int size) {
        if (distanceFunction == L2_DISTANCE) {
            L2Distance.computeL2Distance(originVector, originVectorOffset, firstVector, firstVectorOffset,
                    secondVector, secondVectorOffset,
                    thirdVector, thirdVectorOffset,
                    fourthVector, fourthVectorOffset,
                    result,
                    size);
        } else {
            throw new IllegalStateException("Unknown distance function: " + distanceFunction);
        }
    }


    public static int findClosestVector(final byte distanceFunction, float[][] vectors, float[] vector, int from) {
        var minDistance = Float.MAX_VALUE;
        var minIndex = -1;

        for (int i = 0; i < vectors.length; i++) {
            var centroid = vectors[i];
            var distance = computeDistance(distanceFunction, centroid, vector, from,
                    centroid.length);

            if (distance < minDistance) {
                minDistance = distance;
                minIndex = i;
            }
        }

        return minIndex;
    }

    public static void findClosestVector(final byte distanceFunction, float[][] vectors, float[] vector1,
                                         float[] vector2, float[] vector3, float[] vector4,
                                         int from, int[] result) {
        var minDistance_1 = Float.MAX_VALUE;
        var minDistance_2 = Float.MAX_VALUE;
        var minDistance_3 = Float.MAX_VALUE;
        var minDistance_4 = Float.MAX_VALUE;

        var minIndex_1 = -1;
        var minIndex_2 = -1;
        var minIndex_3 = -1;
        var minIndex_4 = -1;

        var distance = new float[4];

        for (int i = 0; i < vectors.length; i++) {
            var centroid = vectors[i];

            computeDistance(distanceFunction, centroid, 0,
                    vector1, from,
                    vector2, from,
                    vector3, from,
                    vector4, from,
                    distance,
                    centroid.length);

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
}
