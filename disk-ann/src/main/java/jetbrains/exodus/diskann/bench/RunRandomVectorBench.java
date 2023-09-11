package jetbrains.exodus.diskann.bench;

import jetbrains.exodus.diskann.DiskANN;
import jetbrains.exodus.diskann.DotDistanceFunction;
import jetbrains.exodus.diskann.L2PQQuantizer;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class RunRandomVectorBench {
    public static void main(String[] args) throws Exception {
        System.out.println("Reading ground truth...");
        var dbPath = Path.of(PrepareRandomVectorBench.DB_PATH_NAME);
        var vectorDimensions = 768;

        var testDataPath = dbPath.resolve(PrepareRandomVectorBench.TEST_DATA_FILE_NAME);
        var groundTruthPath = dbPath.resolve(PrepareRandomVectorBench.GROUND_TRUTH_FILE_NAME);

        var groundTruthCount = 10_000;
        var groundTruth = new int[groundTruthCount];

        var readBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);

        try (var channel = FileChannel.open(groundTruthPath, StandardOpenOption.READ)) {
            for (var index = 0; index < groundTruthCount; index++) {
                readBuffer.rewind();

                while (readBuffer.remaining() > 0) {
                    channel.read(readBuffer);
                }

                readBuffer.rewind();
                groundTruth[index] = readBuffer.getInt();
            }
        }

        System.out.println("Running queries...");
        var errors = 0;
        var result = new long[1];

        try (var diskANN = new DiskANN("random_index", dbPath, vectorDimensions, DotDistanceFunction.INSTANCE,
                L2PQQuantizer.INSTANCE)) {
            diskANN.loadIndex(400 * 1024 * 1024);

            try (var queryVectors = new PrepareRandomVectorBench.MmapVectorReader(vectorDimensions, testDataPath)) {
                var start = System.nanoTime();
                for (var index = 0; index < groundTruthCount; index++) {
                    var queryVectorSegment = queryVectors.read(index);

                    var queryVector = new float[vectorDimensions];
                    MemorySegment.copy(queryVectorSegment, ValueLayout.JAVA_FLOAT, 0, queryVector,
                            0, vectorDimensions);

                    diskANN.nearest(queryVector, result, 1);
                    if (result[0] != groundTruth[index]) {
                        errors++;
                    }
                }
                var end = System.nanoTime();
                System.out.printf("Queries done in %d ms.%n", (end - start) / 1000000);
            }

            System.out.printf("Errors: %f%% pq errors %f%%%n", 100.0 * errors / groundTruthCount, diskANN.getPQErrorAvg());
        }
    }
}
