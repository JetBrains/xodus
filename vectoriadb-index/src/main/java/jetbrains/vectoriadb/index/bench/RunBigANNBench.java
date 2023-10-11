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
package jetbrains.vectoriadb.index.bench;

import jetbrains.vectoriadb.index.Distance;
import jetbrains.vectoriadb.index.IndexReader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class RunBigANNBench {
    public static void main(String[] args) throws Exception {
        var benchPathStr = System.getProperty("bench.path");
        var benchPath = Path.of(Objects.requireNonNullElse(benchPathStr, "."));

        var queryFileName = "bigann_query.bvecs";
        var queryFilePath = benchPath.resolve(queryFileName);

        if (!Files.exists(queryFilePath) || Files.size(queryFilePath) == 0) {
            var queryArchiveName = "bigann_query.bvecs.gz";
            var queryArchivePath = BenchUtils.downloadBenchFile(benchPath, queryArchiveName);

            BenchUtils.extractGzArchive(queryFilePath, queryArchivePath);
        }

        var bigAnnQueryVectors = BenchUtils.readFBVectors(queryFilePath,
                PrepareBigANNBench.VECTOR_DIMENSIONS, Integer.MAX_VALUE);
        var bigAnnDbDir = benchPath.resolve(PrepareBigANNBench.INDEX_NAME);

        var bigAnnGroundTruth = GenerateGroundTruthBigANNBench.readGroundTruth(benchPath);

        if (bigAnnGroundTruth.length != bigAnnQueryVectors.length) {
            throw new RuntimeException("Ground truth and query vectors count mismatch : " +
                    bigAnnGroundTruth.length + " vs " + bigAnnQueryVectors.length);
        }

        System.out.printf("%d queries for BigANN bench are read%n", bigAnnQueryVectors.length);

        var dataFileName = "bigann_base.bvecs";
        var dataFilePath = benchPath.resolve(dataFileName);

        if (!Files.exists(dataFilePath) || Files.size(dataFilePath) == 0) {
            var baseArchiveName = "bigann_base.bvecs.gz";
            var baseArchivePath = BenchUtils.downloadBenchFile(benchPath, baseArchiveName);

            BenchUtils.extractGzArchive(dataFilePath, baseArchivePath);
        }

        var m1BenchPathProperty = System.getProperty("m1-bench.path");
        var m1BenchPath = Path.of(Objects.requireNonNullElse(m1BenchPathProperty, "."));
        var m1BenchDbDir = m1BenchPath.resolve("vectoriadb-bench");

        var m1BenchSiftsBaseDir = m1BenchPath.resolve("sift");
        var m1QueryFile = m1BenchSiftsBaseDir.resolve("sift_query.fvecs");
        var m1QueryVectors = BenchUtils.readFVectors(m1QueryFile, PrepareBigANNBench.VECTOR_DIMENSIONS);

        try (var indexReader = new IndexReader("test_index", PrepareBigANNBench.VECTOR_DIMENSIONS,
                m1BenchDbDir, 110L * 1024 * 1024 * 1024, Distance.L2)) {
            System.out.println("Reading queries for Sift1M bench...");

            System.out.println(m1QueryVectors.length + " queries for Sift1M bench are read");

            System.out.println("Warming up ...");

            var result = new int[1];
            for (int i = 0; i < 50; i++) {
                for (float[] vector : m1QueryVectors) {
                    indexReader.nearest(vector, result, 1);
                }
            }
        }
        System.out.println("Warm up done.");

        var recallCount = 5;
        var totalRecall = 0.0;
        var totalTime = 0L;

        System.out.println("Loading BigANN index...");
        try (var indexReader = new IndexReader(PrepareBigANNBench.INDEX_NAME, PrepareBigANNBench.VECTOR_DIMENSIONS,
                bigAnnDbDir, 16L * 1024 * 1024 * 1024, Distance.DOT)) {

            System.out.println("Running BigANN bench...");
            var result = new int[recallCount];
            var start = System.nanoTime();
            for (int i = 0; i < bigAnnQueryVectors.length; i++) {
                float[] vector = bigAnnQueryVectors[i];
                indexReader.nearest(vector, result, recallCount);
                totalRecall += recall(result, bigAnnGroundTruth[i], recallCount);
            }
            var end = System.nanoTime();
            totalTime = end - start;
        }

        System.out.printf("BigANN bench done in %d ms, (%d ms per query) recall@%d = %f%n", totalTime / 1000000,
                totalTime / 1000000 / bigAnnQueryVectors.length,
                recallCount,
                totalRecall / bigAnnQueryVectors.length);
    }

    private static double recall(int[] results, int[] groundTruths, int len) {
        assert results.length == groundTruths.length;

        int answers = 0;
        for (var result : results) {
            if (contains(result, groundTruths, len)) {
                answers++;
            }
        }

        return answers * 1.0 / len;
    }

    private static boolean contains(int value, int[] values, int len) {
        for (int i = 0; i < len; i++) {
            if (values[i] == value) {
                return true;
            }
        }
        return false;
    }
}
