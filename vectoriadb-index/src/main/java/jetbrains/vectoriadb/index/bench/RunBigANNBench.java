package jetbrains.vectoriadb.index.bench;

import jetbrains.vectoriadb.index.Distance;
import jetbrains.vectoriadb.index.DistanceFunction;
import jetbrains.vectoriadb.index.IndexReader;
import jetbrains.vectoriadb.index.L2DistanceFunction;
import jetbrains.vectoriadb.index.util.collections.BoundedGreedyVertexPriorityQueue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class RunBigANNBench {
    public static void main(String[] args) throws Exception {
        var vectorDimensions = 128;

        var benchPathStr = System.getProperty("bench.path");
        var benchPath = Path.of(Objects.requireNonNullElse(benchPathStr, "."));

        var gndArchiveName = "bigann_gnd.tar.gz";
        var gndArchivePath = BenchUtils.downloadBenchFile(benchPath, gndArchiveName);

        var gndDirName = "gnd";
        var bigAnnGroundTruthFileName = "idx_1M.ivecs";

        var gndDirPath = benchPath.resolve(gndDirName);
        var bigAnnGroundTruthFile = gndDirPath.resolve(bigAnnGroundTruthFileName);
        Files.createDirectories(gndDirPath);

        if (!Files.exists(bigAnnGroundTruthFile) || Files.size(bigAnnGroundTruthFile) == 0) {
            BenchUtils.extractTarGzArchive(benchPath, gndArchivePath);
        }
        var bigAnnGroundTruth = BenchUtils.readIVectors(bigAnnGroundTruthFile, 1000);

        var queryFileName = "bigann_query.bvecs";
        var queryFilePath = benchPath.resolve(queryFileName);

        var queryArchiveName = "bigann_query.bvecs.gz";
        var queryArchivePath = BenchUtils.downloadBenchFile(benchPath, queryArchiveName);
        if (!Files.exists(queryFilePath) || Files.size(queryFilePath) == 0) {
            BenchUtils.extractGzArchive(queryFilePath, queryArchivePath);
        }

        var bigAnnQueryVectors = BenchUtils.readFBVectors(queryFilePath, vectorDimensions, Integer.MAX_VALUE);
        var bigAnnDbDir = benchPath.resolve("vectoriadb-bigann_index");
        var bigAnnDBName = "test_bigann_index";

        if (bigAnnGroundTruth.length != bigAnnQueryVectors.length) {
            throw new RuntimeException("Ground truth and query vectors count mismatch : " +
                    bigAnnGroundTruth.length + " vs " + bigAnnQueryVectors.length);
        }

        System.out.printf("%d queries for BigANN bench are read%n", bigAnnQueryVectors.length);

        var baseArchiveName = "bigann_base.bvecs.gz";
        var baseArchivePath = BenchUtils.downloadBenchFile(benchPath, baseArchiveName);
        var dataFileName = "bigann_base.bvecs";
        var dataFilePath = benchPath.resolve(dataFileName);

        if (!Files.exists(dataFilePath) || Files.size(dataFilePath) == 0) {
            BenchUtils.extractGzArchive(dataFilePath, baseArchivePath);
        }

        var dataVectors = BenchUtils.readFBVectors(dataFilePath, vectorDimensions, 1_000_000);


//        var m1BenchPathProperty = System.getProperty("m1-bench.path");
//        var m1BenchPath = Path.of(Objects.requireNonNullElse(m1BenchPathProperty, "."));
//        var m1BenchDbDir = m1BenchPath.resolve("vectoriadb-bench");
//
//        var m1BenchSiftsBaseDir = m1BenchPath.resolve("sift");
//        var m1QueryFile = m1BenchSiftsBaseDir.resolve("sift_query.fvecs");
//        var m1QueryVectors = BenchUtils.readFVectors(m1QueryFile, vectorDimensions);

//        try (var indexReader = new IndexReader("test_index", vectorDimensions, m1BenchDbDir,
//                110L * 1024 * 1024 * 1024, Distance.L2)) {
//            System.out.println("Reading queries for Sift1M bench...");
//
//            System.out.println(m1QueryVectors.length + " queries for Sift1M bench are read");
//
//            System.out.println("Warming up ...");
//
//            var result = new int[1];
//            for (int i = 0; i < 50; i++) {
//                for (float[] vector : m1QueryVectors) {
//                    indexReader.nearest(vector, result, 1);
//                }
//            }
//        }
//        System.out.println("Warm up done.");

        var recallCount = 5;
        var totalRecall = 0.0;
        var totalTime = 0L;

        var expectedGnd = calculateGroundTruthVectors(dataVectors, bigAnnQueryVectors,
                L2DistanceFunction.INSTANCE, recallCount);

        System.out.println("Running BigANN bench...");
        try (var indexReader = new IndexReader(bigAnnDBName, vectorDimensions, bigAnnDbDir,
                110L * 1024 * 1024 * 1024, Distance.L2)) {

            var result = new int[recallCount];
            var start = System.nanoTime();
            for (int i = 0; i < bigAnnQueryVectors.length; i++) {
                float[] vector = bigAnnQueryVectors[i];
                indexReader.nearest(vector, result, recallCount);
                totalRecall += recall(result, expectedGnd[i], recallCount);
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

    private static int[][] calculateGroundTruthVectors(float[][] vectors, float[][] queryVectors,
                                                       final DistanceFunction distanceFunction, int itemsPerElement) {
        var cores = Runtime.getRuntime().availableProcessors();
        int maxQueryVectorsPerThread = (queryVectors.length + cores - 1) / cores;
        var groundTruth = new int[vectors.length][itemsPerElement];

        try (var executorService = java.util.concurrent.Executors.newFixedThreadPool(cores)) {
            for (int n = 0; n < cores; n++) {
                var start = n * maxQueryVectorsPerThread;
                var end = Math.min((n + 1) * maxQueryVectorsPerThread, queryVectors.length);

                executorService.submit(() -> {
                    var queryResult = new float[queryVectors[0].length];
                    var vectorResult = new float[queryVectors[0].length];

                    var nearestVectors = new BoundedGreedyVertexPriorityQueue(itemsPerElement);
                    for (int i = start; i < end; i++) {
                        var queryVector = distanceFunction.preProcess(queryVectors[i], queryResult);
                        nearestVectors.clear();

                        for (int j = 0; j < vectors.length; j++) {
                            var vector = distanceFunction.preProcess(vectors[j], vectorResult);
                            var distance = distanceFunction.computeDistance(vector, 0, queryVector,
                                    0, vector.length);
                            nearestVectors.add(j, distance, false, false);
                        }

                        nearestVectors.vertexIndices(groundTruth[i], itemsPerElement);
                    }
                });
            }
        }

        return groundTruth;
    }
}
