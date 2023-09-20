package jetbrains.vectoriadb.index.bench;

import jetbrains.vectoriadb.index.L2DistanceFunction;
import jetbrains.vectoriadb.index.util.collections.BoundedGreedyVertexPriorityQueue;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class GenerateGroundTruthBigANNBench {
    public static final String GROUND_TRUTH_FILE = "ground-truth-" + PrepareBigANNBench.NAME_SUFFIX + ".bin";
    public static final int NEIGHBOURS_COUNT = 5;

    public static void main(String[] args) throws Exception {
        var benchPathStr = System.getProperty("bench.path");
        Path benchPath;

        benchPath = Path.of(Objects.requireNonNullElse(benchPathStr, "."));

        var baseArchiveName = "bigann_base.bvecs.gz";
        var baseArchivePath = BenchUtils.downloadBenchFile(benchPath, baseArchiveName);

        var dataFileName = "bigann_base.bvecs";
        var dataFilePath = benchPath.resolve(dataFileName);

        if (!Files.exists(dataFilePath) || Files.size(dataFilePath) == 0) {
            BenchUtils.extractGzArchive(dataFilePath, baseArchivePath);
        }

        var recordSize = Integer.BYTES + PrepareBigANNBench.VECTOR_DIMENSIONS;

        var queryFileName = "bigann_query.bvecs";
        var queryFilePath = benchPath.resolve(queryFileName);

        var queryArchiveName = "bigann_query.bvecs.gz";
        var queryArchivePath = BenchUtils.downloadBenchFile(benchPath, queryArchiveName);
        if (!Files.exists(queryFilePath) || Files.size(queryFilePath) == 0) {
            BenchUtils.extractGzArchive(queryFilePath, queryArchivePath);
        }

        var bigAnnQueryVectors = BenchUtils.readFBVectors(queryFilePath,
                PrepareBigANNBench.VECTOR_DIMENSIONS, Integer.MAX_VALUE);
        var threads = Runtime.getRuntime().availableProcessors();
        int maxQueryVectorsPerThread = (bigAnnQueryVectors.length + threads - 1) / threads;
        var groundTruth = new int[bigAnnQueryVectors.length][NEIGHBOURS_COUNT];
        var distanceFunction = L2DistanceFunction.INSTANCE;

        var progressCounter = new AtomicInteger(0);
        var progressReportedId = new AtomicInteger(-1);

        System.out.printf("Generating ground truth for %d vectors and %d query vectors using %d threads...%n",
                PrepareBigANNBench.VECTORS_COUNT, bigAnnQueryVectors.length, threads);
        try (var channel = FileChannel.open(dataFilePath, StandardOpenOption.READ)) {
            try (var executorService = Executors.newFixedThreadPool(threads)) {
                for (int n = 0; n < threads; n++) {
                    var start = n * maxQueryVectorsPerThread;
                    var end = Math.min((n + 1) * maxQueryVectorsPerThread, PrepareBigANNBench.VECTORS_COUNT);

                    executorService.submit(() -> {
                        try {
                            var buffer =
                                    ByteBuffer.allocate(
                                            (8 * 64 * 1024 * 1024 / recordSize) * recordSize).order(ByteOrder.LITTLE_ENDIAN);

                            var queryResult = new float[PrepareBigANNBench.VECTOR_DIMENSIONS];
                            var vectorResult = new float[PrepareBigANNBench.VECTOR_DIMENSIONS];

                            var nearestVectors = new BoundedGreedyVertexPriorityQueue(NEIGHBOURS_COUNT);
                            var vector = new float[PrepareBigANNBench.VECTOR_DIMENSIONS];

                            for (int i = start; i < end; i++) {
                                nearestVectors.clear();
                                buffer.clear();

                                while (progressReportedId.get() == -1) {
                                    if (progressReportedId.compareAndSet(-1, start)) {
                                        break;
                                    }
                                }

                                while (buffer.remaining() > 0) {
                                    channel.read(buffer, buffer.position());
                                }
                                buffer.rewind();

                                for (int j = 0; j < PrepareBigANNBench.VECTORS_COUNT; j++) {
                                    if (buffer.remaining() == 0) {
                                        buffer.rewind();

                                        var position = (long) j * recordSize;
                                        while (buffer.remaining() > 0) {
                                            var r = channel.read(buffer, position + buffer.position());
                                            if (r == -1) {
                                                break;
                                            }
                                        }

                                        buffer.clear();
                                    }

                                    var dimensions = buffer.getInt();
                                    if (dimensions != PrepareBigANNBench.VECTOR_DIMENSIONS) {
                                        throw new RuntimeException("Vector dimensions mismatch : " +
                                                dimensions + " vs " + PrepareBigANNBench.VECTOR_DIMENSIONS);
                                    }

                                    for (int k = 0; k < PrepareBigANNBench.VECTOR_DIMENSIONS; k++) {
                                        vector[k] = buffer.get();
                                    }

                                    vector = distanceFunction.preProcess(vector, vectorResult);
                                    var queryVector = distanceFunction.preProcess(bigAnnQueryVectors[i], queryResult);

                                    var distance = distanceFunction.computeDistance(vector, 0, queryVector,
                                            0, vector.length);
                                    nearestVectors.add(j, distance, false, false);
                                }

                                nearestVectors.vertexIndices(groundTruth[i], NEIGHBOURS_COUNT);

                                var progress = progressCounter.incrementAndGet();
                                if (start == progressReportedId.get()) {
                                    System.out.printf("Processed %d query vectors out of %d.%n",
                                            progress, bigAnnQueryVectors.length);
                                }
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } finally {
                            progressReportedId.compareAndSet(start, -1);
                        }
                    });
                }
            }

            System.out.println("Writing ground truth...");
            try (var dataOutputStream = new DataOutputStream(
                    new BufferedOutputStream(Files.newOutputStream(benchPath.resolve(GROUND_TRUTH_FILE)),
                            64 * 1024 * 1024))) {
                for (var groundTruthVector : groundTruth) {
                    for (var neighbour : groundTruthVector) {
                        dataOutputStream.writeInt(neighbour);
                    }
                }
            }

            System.out.printf("Done. Ground truth stored in %s%n",
                    benchPath.resolve(GROUND_TRUTH_FILE).toAbsolutePath());
        }
    }

    public static int[][] readGroundTruth(Path benchPath) throws IOException {
        var truthFile = benchPath.resolve(GROUND_TRUTH_FILE);
        var result = new int[(int) (Files.size(truthFile) / (Integer.BYTES * NEIGHBOURS_COUNT))][NEIGHBOURS_COUNT];
        try (var dataInputStream = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(truthFile),
                        64 * 1024 * 1024))) {
            for (var groundTruthVector : result) {
                for (int i = 0; i < NEIGHBOURS_COUNT; i++) {
                    groundTruthVector[i] = dataInputStream.readInt();
                }
            }
        }

        return result;
    }
}
