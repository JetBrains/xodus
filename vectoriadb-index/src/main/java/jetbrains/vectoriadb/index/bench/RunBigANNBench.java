package jetbrains.vectoriadb.index.bench;

import jetbrains.vectoriadb.index.Distance;
import jetbrains.vectoriadb.index.IndexReader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class RunBigANNBench {
    public static void main(String[] args) throws Exception {
        var vectorDimensions = 128;

        var benchPathStr = System.getProperty("bench.path");
        var benchPath = Path.of(Objects.requireNonNullElse(benchPathStr, "."));

        var baseArchiveName = "bigann_gnd.tar.gz";
        var baseArchivePath = BenchUtils.downloadBenchFile(benchPath, baseArchiveName);

        var queryDir = "gnd";
        var queryFileName = "dis_500M.fvecs";

        var queryDirPath = benchPath.resolve(queryDir);
        Files.createDirectories(queryDirPath);

        var queryFilePath = queryDirPath.resolve(queryFileName);

        if (!Files.exists(queryFilePath) || Files.size(queryFilePath) == 0) {
            BenchUtils.extractGzArchive(benchPath, baseArchivePath);
        }

        System.out.printf("Reading queries for BigANN bench from %s...%n", queryFilePath.toAbsolutePath());
        var bigAnnQueryVectors = BenchUtils.readFVectors(queryFilePath, vectorDimensions);

        var bigAnnGroundTruthFileName = "idx_500M.ivecs";

        var bigAnnGroundTruthFile = queryDirPath.resolve(bigAnnGroundTruthFileName);
        @SuppressWarnings("unused") var bigAnnGroundTruth = BenchUtils.readIVectors(bigAnnGroundTruthFile, 1000);

        System.out.printf("%d queries for BigANN bench are read%n", bigAnnQueryVectors.length);

        var m1BenchPathProperty = System.getProperty("m1-bench.path");
        var m1BenchPath = Path.of(Objects.requireNonNullElse(m1BenchPathProperty, "."));
        var m1BenchDbDir = m1BenchPath.resolve("vectoriadb-bench");

        var m1BenchSiftsBaseDir = m1BenchPath.resolve("sift");
        var m1QueryFile = m1BenchSiftsBaseDir.resolve("sift_query.fvecs");
        var m1QueryVectors = BenchUtils.readFVectors(m1QueryFile, vectorDimensions);

        try (var indexReader = new IndexReader("test_index", vectorDimensions, m1BenchDbDir,
                110L * 1024 * 1024 * 1024, Distance.L2)) {
            System.out.println("Reading queries for Sift1M bench...");

            System.out.println(m1QueryVectors.length + " queries for Sift1M bench are read");

            System.out.println("Warming up ...");

            var result = new int[1];
            for (int i = 0; i < 10; i++) {
                for (float[] vector : m1QueryVectors) {
                    indexReader.nearest(vector, result, 1);
                }
            }
        }
    }
}
