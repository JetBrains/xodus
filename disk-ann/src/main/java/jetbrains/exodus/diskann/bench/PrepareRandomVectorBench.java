package jetbrains.exodus.diskann.bench;

import jetbrains.exodus.diskann.DiskANN;
import jetbrains.exodus.diskann.DotDistanceFunction;
import jetbrains.exodus.diskann.L2PQQuantizer;
import jetbrains.exodus.diskann.VectorReader;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public final class PrepareRandomVectorBench {
    public static void main(String[] args) throws Exception {
        var dbPath = Path.of("vectoriadb-random_index");
        var vectorDimensions = 768;

        var testDataPath = dbPath.resolve("test-data.bin");
        var dataPath = dbPath.resolve("data.bin");
        var groundTruthPath = dbPath.resolve("ground-truth.bin");
        var groundTruthCount = 10_000;
        var vectorsCount = 25_000_000;

        if (!Files.exists(dbPath)) {
            Files.createDirectories(dbPath);
        }

        System.out.printf("Generating %d test vectors with dimension %d...%n", vectorsCount, vectorDimensions);
        generateTestData(vectorDimensions, vectorsCount, dataPath);

        System.out.printf("Generating %d query vectors...%n", groundTruthCount);
        generateTestData(vectorDimensions, groundTruthCount, testDataPath);

        System.out.println("Generating ground truth...");
        generateGroundTruth(vectorDimensions, testDataPath, dataPath, groundTruthPath, groundTruthCount);

        System.out.println("Building index...");

        try (var diskAnn = new DiskANN("random_index", dbPath, vectorDimensions, DotDistanceFunction.INSTANCE,
                L2PQQuantizer.INSTANCE)) {
            diskAnn.buildIndex(4, new MmapVectorReader(vectorDimensions, dataPath), 1024 * 1024);
        }

        System.out.println("Done.");
    }

    @SuppressWarnings("unused")
    private static void generateGroundTruth(int vectorDimensions, Path testDataPath, Path dataPath,
                                            Path groundTruthPath, int groundTruthCount)
            throws IOException, InterruptedException, ExecutionException {

        var cores = Runtime.getRuntime().availableProcessors();
        var futures = new Future[cores];

        try (final ExecutorService executorService = Executors.newFixedThreadPool(cores)) {
            try (var testDataReader = new MmapVectorReader(vectorDimensions, testDataPath)) {
                try (var dataReader = new MmapVectorReader(vectorDimensions, dataPath)) {
                    var maxVectorsPerCore = dataReader.size() / cores;

                    var buffer = ByteBuffer.allocate(vectorDimensions * Integer.BYTES);
                    buffer.order(ByteOrder.LITTLE_ENDIAN);

                    try (var channel = FileChannel.open(groundTruthPath,
                            StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
                        for (int i = 0; i < groundTruthCount; i++) {

                            for (int n = 0; n < cores; n++) {
                                var testVector = testDataReader.read(i);

                                var start = n * maxVectorsPerCore;
                                var end = Math.min(start + maxVectorsPerCore, dataReader.size());

                                futures[n] = executorService.submit(() -> {
                                    var minDistance = Float.MAX_VALUE;
                                    var minIndex = -1;

                                    for (int j = start; j < end; j++) {
                                        var vector = dataReader.read(j);

                                        var distance = DotDistanceFunction.INSTANCE.computeDistance(testVector, 0, vector,
                                                0, vectorDimensions);
                                        if (distance < minDistance) {
                                            minDistance = distance;
                                            minIndex = j;
                                        }
                                    }

                                    return minIndex;
                                });
                            }

                            var minIndex = Integer.MAX_VALUE;
                            for (var future : futures) {
                                var index = (Integer) future.get();

                                if (index < minIndex) {
                                    minIndex = index;
                                }
                            }

                            buffer.rewind();
                            buffer.putInt(minIndex);
                            buffer.rewind();

                            while (buffer.remaining() > 0) {
                                //noinspection ResultOfMethodCallIgnored
                                channel.write(buffer);
                            }
                        }

                        channel.force(true);
                    }
                }
            }
        }
    }

    @SuppressWarnings("unused")
    private static void generateTestData(int vectorDimensions, int count, Path filePath) throws IOException {
        var rnd = ThreadLocalRandom.current();
        var buffer = ByteBuffer.allocate(vectorDimensions * Float.BYTES);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        try (var channel = FileChannel.open(filePath,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            for (int i = 0; i < count; i++) {
                buffer.rewind();

                for (int j = 0; j < vectorDimensions; j++) {
                    buffer.putFloat(rnd.nextFloat());
                }
                buffer.rewind();

                while (buffer.remaining() > 0) {
                    //noinspection ResultOfMethodCallIgnored
                    channel.write(buffer);
                }
            }

            channel.force(true);
        }
    }

    private static final class MmapVectorReader implements VectorReader {
        private final int recordSize;
        private final MemorySegment segment;

        private final Arena arena;

        private final int vectorDimensions;

        private final int size;

        public MmapVectorReader(final int vectorDimensions, Path path) throws IOException {
            this.vectorDimensions = vectorDimensions;
            this.recordSize = Float.BYTES * vectorDimensions;


            arena = Arena.openShared();

            try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
                segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(path), arena.scope());
                this.size = (int) (channel.size() / recordSize);
            }
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public MemorySegment read(int index) {
            return segment.asSlice((long) index * recordSize, (long) Float.BYTES * vectorDimensions);
        }

        @Override
        public void close() {
            arena.close();
        }
    }
}
