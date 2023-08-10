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

import jetbrains.exodus.diskann.siftbench.SiftBenchUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

public class DiskANNTest {
    @Test
    public void testFindLoadedVectors() throws Exception {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!");
        }

        var vectorDimensions = 64;

        var vectorsCount = 10_000;
        var secureRandom = new SecureRandom();
        var seed = ByteBuffer.wrap(secureRandom.generateSeed(8)).getLong();
        try {
            var rnd = new Random(seed);
            var vectors = new float[vectorsCount][];
            for (var i = 0; i < vectorsCount; i++) {
                var vector = new float[vectorDimensions];
                vectors[i] = vector;
            }

            var addedVectors = new HashSet<FloatArrayHolder>();

            for (float[] vector : vectors) {
                var counter = 0;
                do {
                    if (counter > 0) {
                        System.out.println("duplicate vector found " + counter + ", retrying...");
                    }

                    for (var j = 0; j < vector.length; j++) {
                        vector[j] = 10 * rnd.nextFloat();
                    }
                    counter++;
                } while (!addedVectors.add(new FloatArrayHolder(vector)));
            }

            var dbDir = Files.createTempDirectory(Path.of(buildDir), "testFindLoadedVectors");
            dbDir.toFile().deleteOnExit();
            try (var diskANN = new DiskANN("test_index", dbDir, vectorDimensions, Distance.L2_DISTANCE)) {
                var ts1 = System.nanoTime();
                diskANN.buildIndex(new ArrayVectorReader(vectors));
                var ts2 = System.nanoTime();
                System.out.printf("Index built in %d ms.%n", (ts2 - ts1) / 1000000);

                var errorsCount = 0;
                ts1 = System.nanoTime();
                for (var j = 0; j < vectorsCount; j++) {
                    var vector = vectors[j];
                    var result = new long[1];
                    diskANN.nearest(vector, result, 1);
                    Assert.assertEquals("j = $j", 1, result.length);
                    if (j != result[0]) {
                        errorsCount++;
                    }
                }
                ts2 = System.nanoTime();
                var errorPercentage = errorsCount * 100.0 / vectorsCount;

                System.out.printf("Avg. query %d time us, errors: %f%%, pq error %f%%%n",
                        (ts2 - ts1) / 1000 / vectorsCount, errorPercentage, diskANN.getPQErrorAvg());
                Assert.assertTrue("Error percentage is too high " + errorPercentage + " > 0.35", errorPercentage <= 0.35);
                Assert.assertTrue("PQ error is too high " + diskANN.getPQErrorAvg() + " > 15", diskANN.getPQErrorAvg() <= 15);

            }

        } catch (Throwable e) {
            System.out.println("Seed: " + seed);
            throw e;
        }
    }

    @Test
    public void testSearchSift10KVectors() throws IOException {
        runSiftBenchmarks(
                "siftsmall", "siftsmall.tar.gz",
                "siftsmall_base.fvecs", "siftsmall_query.fvecs",
                "siftsmall_groundtruth.ivecs", 128
        );

    }

    @SuppressWarnings("SameParameterValue")
    private void runSiftBenchmarks(
            String siftDir, String siftArchive, String siftBaseName,
            String queryFileName, String groundTruthFileName, int vectorDimensions
    ) throws IOException {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!");
        }

        SiftBenchUtils.downloadSiftBenchmark(siftArchive, buildDir);

        var siftSmallDir = SiftBenchUtils.extractSiftDataSet(siftArchive, buildDir);

        var sifSmallFilesDir = siftSmallDir.toPath().resolve(siftDir);
        var siftSmallBase = sifSmallFilesDir.resolve(siftBaseName);

        System.out.println("Reading data vectors...");
        var vectors = SiftBenchUtils.readFVectors(siftSmallBase, vectorDimensions);

        System.out.printf("%d data vectors loaded with dimension %d%n",
                vectors.length, vectorDimensions);

        System.out.println("Reading queries...");
        var queryFile = sifSmallFilesDir.resolve(queryFileName);
        var queryVectors = SiftBenchUtils.readFVectors(queryFile, vectorDimensions);

        System.out.printf("%d queries are read%n", queryVectors.length);
        System.out.println("Reading ground truth...");

        var groundTruthFile = sifSmallFilesDir.resolve(groundTruthFileName);
        var groundTruth = SiftBenchUtils.readIVectors(groundTruthFile, 100);
        Assert.assertEquals(queryVectors.length, groundTruth.length);

        System.out.println("Ground truth is read");


        System.out.println("Building index...");

        var dbDir = Files.createTempDirectory(Path.of(buildDir), "testSearchSift10KVectors");
        dbDir.toFile().deleteOnExit();

        try (var diskANN = new DiskANN("test_index", dbDir, vectorDimensions, Distance.L2_DISTANCE)) {
            var ts1 = System.nanoTime();
            diskANN.buildIndex(new ArrayVectorReader(vectors));
            var ts2 = System.nanoTime();

            System.out.printf("Index built in %d ms.%n", (ts2 - ts1) / 1000000);
            System.out.println("Searching...");

            var errorsCount = 0;
            ts1 = System.nanoTime();
            for (var index = 0; index < queryVectors.length; index++) {
                var vector = queryVectors[index];
                var result = new long[1];
                diskANN.nearest(vector, result, 1);

                Assert.assertEquals("j = " + index, 1, result.length);
                if (groundTruth[index][0] != result[0]) {
                    errorsCount++;
                }
            }
            ts2 = System.nanoTime();
            var errorPercentage = errorsCount * 100.0 / queryVectors.length;

            System.out.printf("Avg. query time : %d us, errors: %f%%  pq error %f%%%n",
                    (ts2 - ts1) / 1000 / queryVectors.length, errorPercentage, diskANN.getPQErrorAvg());
            Assert.assertTrue("PQ error is too high " + diskANN.getPQErrorAvg() + " > 7.7",
                    diskANN.getPQErrorAvg() <= 7.7);
            Assert.assertTrue("Error percentage is too high " + errorPercentage + " > 1.1", errorPercentage <= 1.1);
        }

    }
}

record FloatArrayHolder(float[] floatArray) {

    @Override
    public int hashCode() {
        return Arrays.hashCode(floatArray);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FloatArrayHolder) {
            return Arrays.equals(floatArray, ((FloatArrayHolder) obj).floatArray);
        }
        return false;
    }
}

record ArrayVectorReader(float[][] vectors) implements VectorReader {
    public int size() {
        return vectors.length;
    }

    public MemorySegment read(int index) {
        var vectorSegment = MemorySegment.ofArray(new byte[vectors[index].length * Float.BYTES]);

        MemorySegment.copy(MemorySegment.ofArray(vectors[index]), 0, vectorSegment, 0,
                (long) vectors[index].length * Float.BYTES);

        return vectorSegment;
    }
}
