package jetbrains.exodus.diskann.bench;

import jetbrains.exodus.diskann.DiskANN;
import jetbrains.exodus.diskann.Distance;
import jetbrains.exodus.diskann.VectorReader;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public class PrepareBigANNBench {
    public static void main(String[] args) {
        var benchPathStr = System.getProperty("bench.path");
        Path benchPath;

        benchPath = Path.of(Objects.requireNonNullElse(benchPathStr, "."));

        try {
            var baseArchiveName = "bigann_base.bvecs.gz";
            var baseArchivePath = BenchUtils.downloadBenchFile(benchPath, baseArchiveName);

            var baseName = "bigann_base.bvecs";
            var basePath = benchPath.resolve(baseName);

            if (!Files.exists(basePath) || Files.size(basePath) == 0) {
                BenchUtils.extractGzArchive(basePath, baseArchivePath);
            }

            var vectorDimensions = 128;

            var dbDir = Files.createDirectories(benchPath.resolve("vectoriadb-bigann_index"));
            var vectorReader = new MmapVectorReader(vectorDimensions, basePath);

            System.out.printf("%d data vectors loaded with dimension %d for BigANN index, " +
                            "building index in directory %s...%n",
                    vectorReader.size(), vectorDimensions, dbDir.toAbsolutePath());

            try (var diskANN = new DiskANN("bigann_index", dbDir, vectorDimensions, Distance.L2_DISTANCE)) {
                var ts1 = System.nanoTime();
                diskANN.buildIndex(50, vectorReader);
                var ts2 = System.nanoTime();

                System.out.printf("Index built in %d ms.%n", (ts2 - ts1) / 1000000);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final class MmapVectorReader implements VectorReader {
        private final int recordSize;
        private final MemorySegment segment;
        private final int vectorsCount;

        private final Arena arena;

        private final int vectorDimensions;

        public MmapVectorReader(final int vectorDimensions, Path path) throws IOException {
            this.vectorDimensions = vectorDimensions;
            this.recordSize = Byte.BYTES * vectorDimensions + Integer.BYTES;
            vectorsCount = (int) (Files.size(path) / recordSize);

            arena = Arena.openShared();

            try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
                segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(path), arena.scope());
            }
        }

        @Override
        public int size() {
            return vectorsCount;
        }

        @Override
        public MemorySegment read(int index) {
            var byteVector = new byte[vectorDimensions * Float.BYTES];
            ByteBuffer buffer = ByteBuffer.wrap(byteVector).order(ByteOrder.nativeOrder());

            var startOffset = (long) index * recordSize + Integer.BYTES;
            var endOffset = startOffset + Byte.BYTES * vectorDimensions;

            for (var i = startOffset; i < endOffset; i++) {
                buffer.putFloat(segment.get(ValueLayout.JAVA_BYTE, i));
            }

            return MemorySegment.ofArray(byteVector);
        }

        @Override
        public void close() {
            arena.close();
        }
    }
}


