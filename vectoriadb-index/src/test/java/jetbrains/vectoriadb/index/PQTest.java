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

import org.junit.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class PQTest extends AbstractVectorsTest {
    @Test
    public void pqTestDotProductError() throws Exception {
        var vectors = loadSift10KVectors();

        var pqParameters = L2PQQuantizer.INSTANCE.calculatePQParameters(SIFT_VECTOR_DIMENSIONS, 32);
        var pqQuantizersCount = pqParameters.pqQuantizersCount;
        var pqSubVectorSize = pqParameters.pqSubVectorSize;

        try (var arena = Arena.openShared()) {
            System.out.println("Generating PQ codes...");
            var pqResult = L2PQQuantizer.INSTANCE.generatePQCodes(pqQuantizersCount, pqSubVectorSize,
                    new PQKMeansTest.ArrayVectorReader(vectors), arena);

            System.out.println("PQ codes generated. Calculation error of distance calculation...");
            var floatVectors = convertPqVectorsIntoFloatVectors(pqResult.pqVectors, pqResult.pqCodesVectors);

            var threadsCount = Runtime.getRuntime().availableProcessors();
            var errorCounter = new AtomicInteger();

            var maxMeasurementsPerThread = vectors.length / threadsCount;
            var errors = new double[maxMeasurementsPerThread * threadsCount];
            var futures = new Future[threadsCount];

            try (var executor = Executors.newFixedThreadPool(threadsCount)) {
                for (int n = 0; n < threadsCount; n++) {
                    var start = n * maxMeasurementsPerThread;

                    futures[n] = executor.submit(() -> {
                        var rnd = ThreadLocalRandom.current();

                        for (int i = start; i < start + maxMeasurementsPerThread; i++) {
                            var randomVectorIndex = rnd.nextInt(vectors.length - 1);

                            if (randomVectorIndex >= i) {
                                randomVectorIndex++;
                            }

                            var randomVector = vectors[randomVectorIndex];
                            var expectedDistance = DotDistanceFunction.INSTANCE.computeDistance(vectors[i], 0,
                                    randomVector, 0, SIFT_VECTOR_DIMENSIONS);
                            var actualDistance = DotDistanceFunction.INSTANCE.computeDistance(floatVectors[i], 0,
                                    randomVector, 0, SIFT_VECTOR_DIMENSIONS);
                            var error = 100 * Math.abs(expectedDistance - actualDistance) / expectedDistance;

                            errors[errorCounter.getAndIncrement()] = error;
                        }
                    });
                }
            }

            for (var future : futures) {
                future.get();
            }

            var errorSum = 0.0;
            for (var error : errors) {
                errorSum += error;
            }

            var deviation = 0.0;
            var average = errorSum / errors.length;

            for (double error : errors) {
                deviation += Math.pow(error - average, 2);
            }
            deviation = Math.sqrt(deviation / errors.length);

            System.out.println("Average error: " + errorSum / errors.length + "%, deviation " + deviation + "%");
        }
    }


    private static float[][] convertPqVectorsIntoFloatVectors(MemorySegment pqVectors, float[][][] pqCodes) {
        var quantizersCount = pqCodes.length;
        var vectorsCount = (int) (pqVectors.byteSize() / quantizersCount);

        var subVectorSize = pqCodes[0][0].length;
        var vectorDimension = quantizersCount * subVectorSize;

        var result = new float[vectorsCount][vectorDimension];
        for (int i = 0, pqIndex = 0; i < vectorsCount; i++) {
            var vector = result[i];

            for (int j = 0; j < vectorDimension / subVectorSize; j++) {
                var code = Byte.toUnsignedInt(pqVectors.get(ValueLayout.JAVA_BYTE, pqIndex));
                var subVector = pqCodes[j][code];

                System.arraycopy(subVector, 0, vector, j * subVectorSize, subVectorSize);
                pqIndex++;
            }
        }

        return result;
    }
}
