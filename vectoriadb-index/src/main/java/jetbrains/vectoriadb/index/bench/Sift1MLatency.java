package jetbrains.vectoriadb.index.bench;

import jetbrains.vectoriadb.index.*;
import jetbrains.vectoriadb.index.diskcache.DiskCache;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

public class Sift1MLatency {
    public static void main(String[] args) throws Exception {
        //amount of samples can not be bigger than 10_000
        var samplesToTest = 2_000;
        var pauseBetweenQueries = 500;
        var diskCacheSize = 60 * 1024 * 1024;
        var pauseAfterWarmup = 16 * 60 * 1_000;
        var gcPause = 60 * 1_000;

        var benchPathStr = System.getProperty("bench.path");
        Path benchPath;

        benchPath = Path.of(Objects.requireNonNullElse(benchPathStr, "."));

        System.out.println("Working directory: " + benchPath.toAbsolutePath());

        var rootDir = benchPath.resolve("sift1m");
        Files.createDirectories(rootDir);

        var siftArchiveName = "sift.tar.gz";
        var vectorDimensions = 128;

        System.out.println("Working directory: " + rootDir.toAbsolutePath());

        var siftArchivePath = BenchUtils.downloadBenchFile(rootDir, siftArchiveName);
        BenchUtils.extractTarGzArchive(rootDir, siftArchivePath);

        var siftDir = rootDir.resolve("sift");
        var siftDataName = "sift_base.fvecs";


        var dbDir = rootDir.resolve("vectoriadb");
        Files.createDirectories(dbDir);

        var indexName = "test_index";
        var ts1 = System.nanoTime();
        {
            var vectors = BenchUtils.readFVectors(siftDir.resolve(siftDataName), vectorDimensions);
            System.out.printf("%d data vectors loaded with dimension %d, building index in directory %s...%n",
                    vectors.length, vectorDimensions, dbDir.toAbsolutePath());


            try (var dataBuilder = DataStore.create(indexName, 128, L2DistanceFunction.INSTANCE, dbDir)) {
                for (int i = 0; i < vectors.length; i++) {
                    var vector = vectors[i];
                    var id = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).putInt(i).array();

                    dataBuilder.add(vector, id);
                }
            }
        }

        var dataLocation = DataStore.dataLocation(indexName, dbDir);

        IndexBuilder.buildIndex(indexName, vectorDimensions,
                dbDir, dataLocation, diskCacheSize,
                Distance.L2, new ConsolePeriodicProgressTracker(5));

        var ts2 = System.nanoTime();
        System.out.printf("Index built in %d ms.%n", (ts2 - ts1) / 1000000);

        System.out.printf("Waiting for GC for %d s....%n", gcPause / 1_000);
        System.gc();
        Thread.sleep(gcPause);

        System.out.printf("Heap used: %d MB%n",
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024);


        try (var diskCache = new DiskCache(diskCacheSize, vectorDimensions,
                IndexBuilder.DEFAULT_MAX_CONNECTIONS_PER_VERTEX)) {
            var timestamps = new long[samplesToTest];
            try (var indexReader = new IndexReader(indexName, vectorDimensions,
                    dbDir, Distance.L2, diskCache)) {
                System.out.println("Reading queries...");
                var queryFile = siftDir.resolve("sift_query.fvecs");
                var queryVectors = BenchUtils.readFVectors(queryFile, vectorDimensions);


                System.out.println(queryVectors.length + " queries are read");

                System.out.println("Ground truth is read, searching...");
                System.out.println("Warming up ...");

                for (int i = 0; i < 10; i++) {
                    for (float[] vector : queryVectors) {
                        indexReader.nearest(vector, 5);
                    }
                    System.out.printf("%d iteration completed %n", i + 1);
                }

                System.out.printf("Waiting for GC and chip cooling for %d s....%n", pauseAfterWarmup / 1_000);
                System.gc();
                Thread.sleep(pauseAfterWarmup);

                System.out.println("Benchmark ...");

                final long pid = ProcessHandle.current().pid();
                System.out.println("PID: " + pid);

                for (var i = 0; i < timestamps.length; i++) {
                    var vector = queryVectors[i];

                    ts1 = System.nanoTime();
                    indexReader.nearest(vector, 5);
                    ts2 = System.nanoTime();
                    timestamps[i] = ts2 - ts1;

                    Thread.sleep(pauseBetweenQueries);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            Arrays.sort(timestamps);
            System.out.printf("Median latency: %d ms%n", timestamps[timestamps.length / 2] / 1000000);
            System.out.printf("90%% latency: %d ms%n", timestamps[(int) (timestamps.length * 0.9)] / 1000000);
            System.out.printf("95%% latency: %d ms%n", timestamps[(int) (timestamps.length * 0.95)] / 1000000);
            System.out.printf("99%% latency: %d ms%n", timestamps[(int) (timestamps.length * 0.99)] / 1000000);
            System.out.printf("99.9%% latency: %d ms%n", timestamps[(int) (timestamps.length * 0.999)] / 1000000);
            System.out.printf("Max latency: %d ms%n", timestamps[timestamps.length - 1] / 1000000);
        }
        System.out.printf("Waiting for GC for %d s....%n", gcPause);
        System.gc();
        Thread.sleep(gcPause);

        System.out.printf("Heap used: %d MB%n",
                (Runtime.getRuntime().totalMemory() -
                        Runtime.getRuntime().freeMemory()) / 1024 / 1024);
    }
}
