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
package jetbrains.vectoriadb.index.bench;

import jetbrains.vectoriadb.index.Distance;
import jetbrains.vectoriadb.index.IndexBuilder;
import jetbrains.vectoriadb.index.DataStore;
import jetbrains.vectoriadb.index.IndexReader;
import jetbrains.vectoriadb.index.L2DistanceFunction;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

import java.io.EOFException;
import java.io.IOException;
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

        extractTarGzArchive(rootDir, siftArchivePath);
        var siftsBaseDir = rootDir.resolve(siftDir);

        var vectors = readFVectors(siftsBaseDir.resolve(siftBaseName), vectorDimensions);
        var dbDir = Files.createDirectory(rootDir.resolve("vectoriadb-bench"));

        System.out.printf("%d data vectors loaded with dimension %d, building index in directory %s...%n",
                vectors.length, vectorDimensions, dbDir.toAbsolutePath());

        var ts1 = System.nanoTime();

        Path dataLocation;
        try (var dataBuilder = DataStore.create("test_index", 128, L2DistanceFunction.INSTANCE, dbDir)) {
            for (var vector : vectors) {
                dataBuilder.add(vector);
            }
            dataLocation = dataBuilder.dataLocation();
        }

        IndexBuilder.buildIndex("test_index", 128,
                dbDir, dataLocation, 60L * 1024 * 1024 * 1024,
                Distance.L2);

        var ts2 = System.nanoTime();
        System.out.printf("Index built in %d ms.%n", (ts2 - ts1) / 1000000);

        try (var indexReader = new IndexReader("test_index", 128, dbDir,
                100L * 1024 * 1024 * 1024, Distance.L2)) {
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

            var result = new int[1];
            for (int i = 0; i < 10; i++) {
                for (float[] vector : queryVectors) {
                    indexReader.nearest(vector, result, 1);
                }
            }

            System.out.println("Benchmark ...");

            final long pid = ProcessHandle.current().pid();
            System.out.println("PID: " + pid);


            for (int i = 0; i < 50; i++) {
                ts1 = System.nanoTime();
                var errorsCount = 0;
                for (var index = 0; index < queryVectors.length; index++) {
                    var vector = queryVectors[index];
                    indexReader.nearest(vector, result, 1);
                    if (groundTruth[index][0] != result[0]) {
                        errorsCount++;
                    }
                }
                ts2 = System.nanoTime();
                var errorPercentage = errorsCount * 100.0 / queryVectors.length;

                System.out.printf("Avg. query time : %d us, errors: %f%%, cache hits %d%%%n",
                        (ts2 - ts1) / 1000 / queryVectors.length,
                        errorPercentage, indexReader.hits());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void extractTarGzArchive(Path rootDir, Path archivePath) throws IOException {
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

            var vectorsCount =
                    (int) (channel.size() / (Float.BYTES * vectorDimensions + Integer.BYTES));
            var vectors = new float[vectorsCount][];
            for (var i = 0; i < vectorsCount; i++) {
                vectorBuffer.rewind();
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

    public static float[][] readFBVectors(Path path, int vectorDimensions) throws IOException {
        try (var channel = FileChannel.open(path)) {
            var vectorBuffer = ByteBuffer.allocate(vectorDimensions + Integer.BYTES);
            vectorBuffer.order(ByteOrder.LITTLE_ENDIAN);

            var vectorsCount =
                    (int) (channel.size() / (vectorDimensions + Integer.BYTES));

            var vectors = new float[vectorsCount][];

            for (var i = 0; i < vectorsCount; i++) {
                vectorBuffer.rewind();
                readFully(channel, vectorBuffer);
                vectorBuffer.rewind();

                vectorBuffer.position(Integer.BYTES);

                var vector = new float[vectorDimensions];
                for (var j = 0; j < vector.length; j++) {
                    vector[j] = vectorBuffer.get();
                }
                vectors[i] = vector;
            }

            return vectors;
        }
    }

    @SuppressWarnings("SameParameterValue")
    public static int[][] readIVectors(Path siftSmallBase, int vectorDimensions) throws IOException {
        try (var channel = FileChannel.open(siftSmallBase)) {
            var vectorBuffer = ByteBuffer.allocate(Integer.BYTES * vectorDimensions + Integer.BYTES);
            vectorBuffer.order(ByteOrder.LITTLE_ENDIAN);

            var vectorsCount =
                    (int) (channel.size() / ((long) Integer.BYTES * vectorDimensions + Integer.BYTES));
            var vectors = new int[vectorsCount][];
            for (var i = 0; i < vectorsCount; i++) {
                vectorBuffer.rewind();
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

