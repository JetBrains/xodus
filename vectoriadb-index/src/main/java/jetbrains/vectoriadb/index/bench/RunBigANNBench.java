package jetbrains.vectoriadb.index.bench;

import jetbrains.vectoriadb.index.Distance;
import jetbrains.vectoriadb.index.IndexReader;

import java.nio.file.Path;
import java.util.Objects;

public class RunBigANNBench {
    public static void main(String[] args) throws Exception {
        var m1BenchPathProperty = System.getProperty("m1-bench.path");
        var m1BenchPath = Path.of(Objects.requireNonNullElse(m1BenchPathProperty, "."));
        var m1BenchDbDir = m1BenchPath.resolve("vectoriadb-bench");

        var vectorDimensions = 128;

        var m1BenchSiftsBaseDir = m1BenchPath.resolve("sift");
        var m1QueryFile = m1BenchSiftsBaseDir.resolve("sift_query.fvecs");
        var queryVectors = BenchUtils.readFVectors(m1QueryFile, vectorDimensions);

        try (var indexReader = new IndexReader("test_index", vectorDimensions, m1BenchDbDir,
                100L * 1024 * 1024 * 1024, Distance.L2)) {
            System.out.println("Reading queries for Sift1M bench...");

            System.out.println(queryVectors.length + " queries for Sift1M bench are read");

            System.out.println("Warming up ...");

            var result = new int[1];
            for (int i = 0; i < 10; i++) {
                for (float[] vector : queryVectors) {
                    indexReader.nearest(vector, result, 1);
                }
            }
        }
    }
}
