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

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorSpecies;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.rng.simple.RandomSource;

import java.util.Arrays;
import java.util.BitSet;

final class KMeansMiniBatchSGD {
    private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

    final float[][] centroids;
    final int k;
    final VectorReader vectorReader;
    final int subVecOffset;

    final int[] centroidsSamplesCount;

    private int currentIndex;
    private int iteration;

    KMeansMiniBatchSGD(int k, int subVecOffset, int subVecSize, VectorReader vectorReader) {
        this.k = k;
        this.vectorReader = vectorReader;
        this.subVecOffset = subVecOffset;
        this.centroidsSamplesCount = new int[k];


        var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();

        var size = vectorReader.size();
        if (size <= k) {
            centroids = new float[size][subVecSize];
            for (int i = 0; i < size; i++) {
                var vector = vectorReader.read(i);
                centroids[i] = Arrays.copyOfRange(vector, subVecOffset, subVecOffset + subVecSize);
            }
        } else if (size < 4 * k) {
            centroids = new float[k][subVecSize];
            var indexes = new int[size];

            for (int i = 0; i < size; i++) {
                indexes[i] = i;
            }

            ArrayUtils.shuffle(indexes);

            for (int i = 0; i < k; i++) {
                var centroidIndex = indexes[i];
                var vector = vectorReader.read(centroidIndex);
                centroids[i] = Arrays.copyOfRange(vector, subVecOffset, subVecOffset + subVecSize);
            }
        } else {
            centroids = new float[k][subVecSize];
            var bitSet = new BitSet(size);
            for (int i = 0; i < k; i++) {
                int centroidIndex;
                do {
                    centroidIndex = rng.nextInt(size);
                } while (bitSet.get(centroidIndex));
                bitSet.set(centroidIndex);

                var vector = vectorReader.read(centroidIndex);
                centroids[i] = Arrays.copyOfRange(vector, subVecOffset, subVecOffset + subVecSize);
            }
        }


    }

    int nextIteration(@SuppressWarnings("SameParameterValue") int batchSize, byte distanceFunction) {
        if ((batchSize & 3) != 0) {
            throw new IllegalArgumentException("Batch size must be a multiple of 3");
        }

        var actualBatchSize = Math.min(currentIndex + batchSize, vectorReader.size()) - currentIndex;
        if ((actualBatchSize & 3) == 0) {
            return nextIterationFastPath(actualBatchSize, distanceFunction);
        } else {
            return nextIterationSlowPath(actualBatchSize, distanceFunction);
        }
    }

    private int nextIterationSlowPath(int batchSize, byte distanceFunction) {
        var vectors = new float[batchSize][];
        var centroidIds = new int[batchSize];

        for (int i = 0; i < batchSize; i++) {
            var vector = vectorReader.read(currentIndex + i);
            vectors[i] = vector;
            centroidIds[i] = Distance.findClosestVector(distanceFunction, centroids, vector, subVecOffset);
        }


        for (int i = 0; i < batchSize; i++) {
            var centroid = centroidIds[i];
            centroidsSamplesCount[centroid]++;

            var learningRate = 1.0f / centroidsSamplesCount[centroid];

            var after = computeGradientStep(centroids[centroid], vectors[i], subVecOffset, learningRate);
            centroids[centroid] = after;
        }

        currentIndex += batchSize;
        if (currentIndex == vectorReader.size()) {
            iteration++;
        }

        return iteration;
    }

    private int nextIterationFastPath(int batchSize, byte distanceFunction) {
        var vectors = new float[batchSize][];
        var centroidIds = new int[batchSize];
        var result = new int[4];

        for (int i = 0; i < batchSize; i += 4) {
            var index = currentIndex + i;

            var vector_1 = vectorReader.read(index);
            var vector_2 = vectorReader.read(index + 1);
            var vector_3 = vectorReader.read(index + 2);
            var vector_4 = vectorReader.read(index + 3);

            vectors[i] = vector_1;
            vectors[i + 1] = vector_2;
            vectors[i + 2] = vector_3;
            vectors[i + 3] = vector_4;

            Distance.findClosestVector(distanceFunction, centroids, vector_1, vector_2,
                    vector_3, vector_4, subVecOffset, result);

            centroidIds[i] = result[0];
            centroidIds[i + 1] = result[1];
            centroidIds[i + 2] = result[2];
            centroidIds[i + 3] = result[3];
        }


        for (int i = 0; i < batchSize; i++) {
            var centroid = centroidIds[i];
            centroidsSamplesCount[centroid]++;

            var learningRate = 1.0f / centroidsSamplesCount[centroid];

            var after = computeGradientStep(centroids[centroid], vectors[i], subVecOffset, learningRate);
            centroids[centroid] = after;
        }

        currentIndex += batchSize;
        if (currentIndex == vectorReader.size()) {
            iteration++;
        }

        return iteration;
    }

    static float[] computeGradientStep(float[] centroid, float[] point, int pointOffset, float learningRate) {
        var result = new float[centroid.length];
        var index = 0;
        var learningRateVector = FloatVector.broadcast(SPECIES, learningRate);
        var loopBound = SPECIES.loopBound(centroid.length);
        var step = SPECIES.length();

        for (; index < loopBound; index += step) {
            var centroidVector = FloatVector.fromArray(SPECIES, centroid, index);
            var pointVector = FloatVector.fromArray(SPECIES, point, index + pointOffset);

            var diff = pointVector.sub(centroidVector);
            centroidVector = diff.fma(learningRateVector, centroidVector);
            centroidVector.intoArray(result, index);
        }

        for (; index < centroid.length; index++) {
            result[index] = centroid[index] + learningRate * (point[index + pointOffset] - centroid[index]);
        }

        return result;
    }
}
