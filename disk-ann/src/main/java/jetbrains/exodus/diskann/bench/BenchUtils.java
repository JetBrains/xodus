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
package jetbrains.exodus.diskann.bench;

import jetbrains.exodus.diskann.DiskANN;
import jetbrains.exodus.diskann.Distance;
import jetbrains.exodus.diskann.VectorReader;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.jetbrains.annotations.NotNull;

import java.io.EOFException;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

final class BenchUtils {
    static void runSiftBenchmarks(
            Path rootDir, String siftDir, String siftArchiveName, String siftBaseName,
            String queryFileName,
            String groundTruthFileName, int vectorDimensions
    ) throws IOException {
        System.out.println("Working directory: " + rootDir.toAbsolutePath());

        var siftArchivePath = downloadBenchFile(rootDir, siftArchiveName);


        extractTarArchive(rootDir, siftArchivePath);
        var siftsBaseDir = rootDir.resolve(siftDir);

        var vectors = readRawFVectors(siftsBaseDir.resolve(siftBaseName), vectorDimensions);
        var dbDir = Files.createTempDirectory("vectoriadb-bench");
        dbDir.toFile().deleteOnExit();

        System.out.printf("%d data vectors loaded with dimension %d, building index in directory %s...%n",
                vectors.length, vectorDimensions, dbDir.toAbsolutePath());

        try (var diskANN = new DiskANN("test_index", dbDir, vectorDimensions, Distance.L2_DISTANCE)) {
            var ts1 = System.nanoTime();
            diskANN.buildIndex(16, new ArrayVectorReader(vectors));
            var ts2 = System.nanoTime();

            System.out.printf("Index built in %d ms.%n", (ts2 - ts1) / 1000000);
        }

        try (var diskANN = new DiskANN("test_index", dbDir, vectorDimensions, Distance.L2_DISTANCE)) {
            diskANN.loadIndex();
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

            var result = new long[1];
            for (int i = 0; i < 10; i++) {
                for (float[] vector : queryVectors) {
                    diskANN.nearest(vector, result, 1);
                }
            }

            System.out.println("Benchmark ...");

            final long pid = ProcessHandle.current().pid();
            System.out.println("PID: " + pid);


            for (int i = 0; i < 50; i++) {
                var ts1 = System.nanoTime();
                var errorsCount = 0;
                for (var index = 0; index < queryVectors.length; index++) {
                    var vector = queryVectors[index];
                    diskANN.nearest(vector, result, 1);
                    if (groundTruth[index][0] != result[0]) {
                        errorsCount++;
                    }
                }
                var ts2 = System.nanoTime();
                var errorPercentage = errorsCount * 100.0 / queryVectors.length;

                System.out.printf("Avg. query time : %d us, errors: %f%% pq error %f%%%n", (ts2 - ts1) / 1000 / queryVectors.length,
                        errorPercentage, diskANN.getPQErrorAvg());
                diskANN.resetPQErrorStat();
            }

            diskANN.deleteIndex();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void extractTarArchive(Path rootDir, Path archivePath) throws IOException {
        System.out.println("Extracting " + archivePath.getFileName() + " into " + rootDir);

        try (var fis = Files.newInputStream(archivePath)) {
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

        System.out.printf("%s extracted%n", archivePath.getFileName());
    }

    public static void extractGzArchive(Path targetPath, Path archivePath) throws IOException {
        System.out.println("Extracting " + archivePath.getFileName() + " into " + targetPath.getFileName());

        try (var fis = Files.newInputStream(archivePath)) {
            try (var giz = new GzipCompressorInputStream(fis)) {
                Files.copy(giz, targetPath, StandardCopyOption.REPLACE_EXISTING);
            }
        }

        System.out.printf("%s extracted%n", archivePath.getFileName());
    }


    public static Path downloadBenchFile(Path rootDir, String benchArchiveName) throws IOException {
        var benchArchivePath = rootDir.resolve(benchArchiveName);

        if (Files.exists(benchArchivePath)) {
            System.out.println(benchArchiveName + " already exists in " + rootDir);
        } else {
            System.out.println("Downloading " + benchArchiveName +
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
            try (var fos = Files.newOutputStream(benchArchivePath)) {
                ftpClient.retrieveFile("/local/texmex/corpus/" + benchArchiveName, fos);
            } finally {
                ftpClient.logout();
                ftpClient.disconnect();
            }

            System.out.println(benchArchiveName + " downloaded");
        }

        return benchArchivePath;
    }

    public static float[][] readFVectors(Path path, int vectorDimensions) throws IOException {
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

    public static byte[][] readRawFVectors(Path path, int vectorDimensions) throws IOException {
        try (var channel = FileChannel.open(path)) {

            var vectorBuffer = ByteBuffer.allocate(Float.BYTES * vectorDimensions + Integer.BYTES);
            vectorBuffer.order(ByteOrder.LITTLE_ENDIAN);

            readFully(channel, vectorBuffer);
            vectorBuffer.rewind();

            var vectorsCount =
                    (int) (channel.size() / (Float.BYTES * vectorDimensions + Integer.BYTES));

            var vectors = new byte[vectorsCount][];
            {
                var vector = readFloatVector(vectorDimensions, vectorBuffer);
                vectors[0] = vector;
            }

            for (var i = 1; i < vectorsCount; i++) {
                vectorBuffer.clear();
                readFully(channel, vectorBuffer);
                vectorBuffer.rewind();

                vectorBuffer.position(Integer.BYTES);

                var vector = readFloatVector(vectorDimensions, vectorBuffer);
                vectors[i] = vector;
            }
            return vectors;
        }
    }

    @NotNull
    private static byte[] readFloatVector(int vectorDimensions, ByteBuffer vectorBuffer) {
        var vector = new byte[vectorDimensions * Float.BYTES];
        for (var i = 0; i < vector.length; i++) {
            vector[i] = vectorBuffer.get();
        }
        return vector;
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

record ArrayVectorReader(byte[][] vectors) implements VectorReader {
    public int size() {
        return vectors.length;
    }

    public MemorySegment read(int index) {
        return MemorySegment.ofArray(vectors[index]);
    }

    @Override
    public void close() {
    }
}

