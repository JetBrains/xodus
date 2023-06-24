package jetbrains.exodus.diskann.bench;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import it.unimi.dsi.fastutil.longs.LongObjectImmutablePair;
import jetbrains.exodus.diskann.DiskANN;
import jetbrains.exodus.diskann.VectorReader;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

final class BenchUtils {
    static void runSiftBenchmarks(
            Path rootDir, String siftDir, String siftArchiveName, String siftBaseName,
            String queryFileName,
            String groundTruthFileName, int vectorDimensions
    ) throws IOException {
        System.out.println("Working directory: " + rootDir.toAbsolutePath());
        var siftArchivePath = rootDir.resolve(siftArchiveName);
        if (Files.exists(siftArchivePath)) {
            System.out.println(siftArchiveName + " already exists in " + rootDir);
        } else {
            System.out.println("Downloading " + siftArchiveName +
                    " from ftp.irisa.fr into " + rootDir);

            var ftpClient = new FTPClient();
            ftpClient.connect("ftp.irisa.fr");
            ftpClient.enterLocalPassiveMode();
            var loggedIdn = ftpClient.login("anonymous", "anonymous");
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            if (!loggedIdn) {
                throw new IllegalStateException("Failed to login to ftp.irisa.fr");
            }

            System.out.println("Logged in to ftp.irisa.fr");
            try (var fos = Files.newOutputStream(siftArchivePath)) {
                ftpClient.retrieveFile("/local/texmex/corpus/" + siftArchiveName, fos);
            } finally {
                ftpClient.logout();
                ftpClient.disconnect();
            }

            System.out.println(siftArchiveName + " downloaded");
        }

        System.out.println("Extracting " + siftArchiveName + " into " + rootDir);

        try (var fis = Files.newInputStream(rootDir.resolve(siftArchiveName))) {
            try (var giz = new GzipCompressorInputStream(fis)) {
                try (var tar = new TarArchiveInputStream(giz)) {
                    var entry = tar.getNextTarEntry();

                    while (entry != null) {
                        var name = entry.getName();
                        if (name.endsWith(".fvecs") || name.endsWith(".ivecs")) {
                            System.out.printf("Extracting %s%n", name);
                            var file = rootDir.resolve(name);
                            if (!Files.exists(file.getParent())) {
                                Files.createDirectories(file.getParent());
                            }

                            try (var fos = Files.newOutputStream(file)) {
                                IOUtils.copy(tar, fos);
                            }
                        }
                        entry = tar.getNextTarEntry();
                    }
                }
            }
        }

        System.out.printf("%s extracted%n", siftArchiveName);

        var siftsBaseDir = rootDir.resolve(siftDir);
        var vectors = readFVectors(siftsBaseDir.resolve(siftBaseName), vectorDimensions);

        System.out.printf("%d data vectors loaded with dimension %d, building index...%n",
                vectors.length, vectorDimensions);

        try (var diskANN = new DiskANN("test index", vectorDimensions, DiskANN.L2_DISTANCE)) {
            var ts1 = System.nanoTime();
            diskANN.buildIndex(new ArrayVectorReader(vectors));
            var ts2 = System.nanoTime();

            System.out.println("Index built in " + (ts2 - ts1) / 1000000 + "  ms");

            System.out.println("Reading queries...");
            var queryFile = siftsBaseDir.resolve(queryFileName);
            var queryVectors = readFVectors(queryFile, vectorDimensions);

            System.out.println(queryVectors.length + " queries are read");
            System.out.println("Reading ground truth...");
            var groundTruthFile = siftsBaseDir.resolve(groundTruthFileName);
            var groundTruth = readIVectors(groundTruthFile, 100);

            System.out.println("Ground truth is read, searching...");
            System.out.println("Warming up ...");

            //give GC chance to collect garbage
            Thread.sleep(60 * 1000);

            for (int i = 0; i < 10; i++) {
                for (float[] vector : queryVectors) {
                    diskANN.nearest(vector, 1);
                }
            }

            System.out.println("Benchmark ...");

            final long pid = ProcessHandle.current().pid();
            System.out.println("PID: " + pid);


            var iterationsCount = 100_000;
            for (int i = 0; i < iterationsCount; i++) {
                ts1 = System.nanoTime();
                var errorsCount = 0;
                for (var index = 0; index < queryVectors.length; index++) {
                    var vector = queryVectors[index];
                    var result = diskANN.nearest(vector, 1);
                    if (groundTruth[index][0] != result[0]) {
                        errorsCount++;
                    }
                }
                ts2 = System.nanoTime();
                var errorPercentage = errorsCount * 100.0 / queryVectors.length;

                System.out.printf("Avg. query time : %d us, errors: %f%%%n", (ts2 - ts1) / 1000 / queryVectors.length,
                        errorPercentage);

            }


        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static float[][] readFVectors(Path path, int vectorDimensions) throws IOException {
        try (var channel = FileChannel.open(path)) {

            var vectorBuffer = ByteBuffer.allocate(Float.BYTES * vectorDimensions + Integer.BYTES);
            vectorBuffer.order(ByteOrder.LITTLE_ENDIAN);

            readFully(channel, vectorBuffer);
            vectorBuffer.rewind();

            var vectorsCount =
                    (int) (channel.size() / (Float.BYTES * vectorDimensions + Integer.BYTES));

            var vectors = new float[vectorsCount][];
            {
                var vector = new float[vectorDimensions];
                for (var i = 0; i < vector.length; i++) {
                    vector[i] = vectorBuffer.getFloat();
                }
                vectors[0] = vector;
            }

            for (var i = 1; i < vectorsCount; i++) {
                vectorBuffer.clear();
                readFully(channel, vectorBuffer);
                vectorBuffer.rewind();

                vectorBuffer.position(Integer.BYTES);

                var vector = new float[vectorDimensions];
                for (var j = 0; j < vector.length; j++) {
                    vector[j] = vectorBuffer.getFloat();
                }
                vectors[i] = vector;
            }
            return vectors;
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static int[][] readIVectors(Path siftSmallBase, int vectorDimensions) throws IOException {
        try (var channel = FileChannel.open(siftSmallBase)) {
            var vectorBuffer = ByteBuffer.allocate(Integer.BYTES * vectorDimensions + Integer.BYTES);
            vectorBuffer.order(ByteOrder.LITTLE_ENDIAN);

            readFully(channel, vectorBuffer);
            vectorBuffer.rewind();

            var vectorsCount =
                    (int) (channel.size() / ((long) Integer.BYTES * vectorDimensions + Integer.BYTES));
            var vectors = new int[vectorsCount][];

            {
                var vector = new int[vectorDimensions];
                for (var i = 0; i < vector.length; i++) {
                    vector[i] = vectorBuffer.getInt();
                }
                vectors[0] = vector;
            }

            for (var i = 1; i < vectorsCount; i++) {
                vectorBuffer.clear();
                readFully(channel, vectorBuffer);
                vectorBuffer.rewind();

                vectorBuffer.position(Integer.BYTES);

                var vector = new int[vectorDimensions];
                for (var j = 0; j < vector.length; j++) {
                    vector[j] = vectorBuffer.getInt();
                }
                vectors[i] = vector;
            }
            return vectors;
        }
    }

    private static void readFully(FileChannel siftSmallBaseChannel, ByteBuffer vectorBuffer) throws IOException {
        while (vectorBuffer.remaining() > 0) {
            var r = siftSmallBaseChannel.read(vectorBuffer);
            if (r < 0) {
                throw new EOFException();
            }
        }
    }
}

record ArrayVectorReader(float[][] vectors) implements VectorReader {
    public long size() {
        return vectors.length;
    }

    public LongObjectImmutablePair<float[]> read(long index) {
        return new LongObjectImmutablePair<>(index, vectors[(int) index]);
    }
}

