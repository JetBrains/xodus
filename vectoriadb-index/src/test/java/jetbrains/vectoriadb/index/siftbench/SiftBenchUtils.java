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
package jetbrains.vectoriadb.index.siftbench;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class SiftBenchUtils {
    public static float[][] readFVectors(Path siftSmallBase, int vectorDimensions) throws IOException {
        try (var siftSmallBaseChannel = FileChannel.open(siftSmallBase)) {
            var vectorBuffer = ByteBuffer.allocate(Float.BYTES * vectorDimensions + Integer.BYTES);
            vectorBuffer.order(ByteOrder.LITTLE_ENDIAN);

            readFully(siftSmallBaseChannel, vectorBuffer);
            vectorBuffer.rewind();

            Assert.assertEquals(vectorDimensions, vectorBuffer.getInt());

            var vectorsCount =
                    (int) (siftSmallBaseChannel.size() / ((long) Float.BYTES * vectorDimensions + Integer.BYTES));
            var vectors = new float[vectorsCount][vectorDimensions];

            {
                var vector = vectors[0];
                for (var i = 0; i < vector.length; i++) {
                    vector[i] = vectorBuffer.getFloat();
                }
            }

            for (var i = 1; i < vectorsCount; i++) {
                vectorBuffer.clear();
                readFully(siftSmallBaseChannel, vectorBuffer);
                vectorBuffer.rewind();

                vectorBuffer.position(Integer.BYTES);

                var vector = vectors[i];
                for (var j = 0; j < vector.length; j++) {
                    vector[j] = vectorBuffer.getFloat();
                }
            }

            return vectors;
        }
    }

    @SuppressWarnings("SameParameterValue")
    public static int[][] readIVectors(Path siftSmallBase, int vectorDimensions) throws IOException {
        try (var siftSmallBaseChannel = FileChannel.open(siftSmallBase)) {
            var vectorBuffer = ByteBuffer.allocate(Integer.BYTES * vectorDimensions + Integer.BYTES);
            vectorBuffer.order(ByteOrder.LITTLE_ENDIAN);

            readFully(siftSmallBaseChannel, vectorBuffer);
            vectorBuffer.rewind();

            Assert.assertEquals(vectorDimensions, vectorBuffer.getInt());

            var vectorsCount =
                    (int) (siftSmallBaseChannel.size() / (Integer.BYTES * vectorDimensions + Integer.BYTES));
            var vectors = new int[vectorsCount][vectorDimensions];
            {
                var vector = vectors[0];
                for (var i = 0; i < vector.length; i++) {
                    vector[i] = vectorBuffer.getInt();
                }
            }

            for (var i = 1; i < vectorsCount; i++) {
                vectorBuffer.clear();
                readFully(siftSmallBaseChannel, vectorBuffer);
                vectorBuffer.rewind();

                vectorBuffer.position(Integer.BYTES);

                var vector = vectors[i];
                for (var j = 0; j < vector.length; j++) {
                    vector[j] = vectorBuffer.getInt();
                }
            }
            return vectors;
        }
    }

    public static void readFully(FileChannel siftSmallBaseChannel, ByteBuffer vectorBuffer) throws IOException {
        while (vectorBuffer.remaining() > 0) {
            var r = siftSmallBaseChannel.read(vectorBuffer);
            Assert.assertTrue(r >= 0);
        }
    }

    @NotNull
    public static File extractSiftDataSet(String siftArchive, String buildDir) throws IOException {
        var siftSmallDir = createTempDir();
        System.out.printf("Extracting %s into %s%n", siftArchive, siftSmallDir.getAbsolutePath());

        try (var fis = new FileInputStream(new File(buildDir).toPath().resolve(siftArchive).toFile())) {
            try (var giz = new GzipCompressorInputStream(fis)) {
                try (var tar = new TarArchiveInputStream(giz)) {
                    var entry = tar.getNextTarEntry();
                    while (entry != null) {
                        var name = entry.getName();
                        if (name.endsWith(".fvecs") || name.endsWith(".ivecs")) {
                            System.out.printf("Extracting %s%n", name);
                            var file = new File(siftSmallDir, name);
                            if (!file.getParentFile().exists()) {
                                //noinspection ResultOfMethodCallIgnored
                                file.getParentFile().mkdirs();
                            }

                            try (var fos = new FileOutputStream(file)) {
                                IOUtils.copy(tar, fos);
                            }
                        }
                        entry = tar.getNextTarEntry();
                    }
                }
            }
        }

        System.out.printf("%s extracted%n", siftArchive);
        return siftSmallDir;
    }

    public static void downloadSiftBenchmark(String siftArchive, String buildDir) throws IOException {
        var siftArchivePath = new File(buildDir, siftArchive);
        if (siftArchivePath.exists()) {
            System.out.printf("%s already exists in %s%n", siftArchive, buildDir);
        } else {
            System.out.printf("Downloading %s from ftp.irisa.fr into %s%n", siftArchive, buildDir);

            var ftpClient = new FTPClient();


            ftpClient.connect("ftp.irisa.fr");

            var loggedIdn = ftpClient.login("anonymous", "anonymous");
            Assert.assertTrue(loggedIdn);
            Assert.assertTrue(ftpClient.setFileType(FTP.BINARY_FILE_TYPE));

            System.out.println("Logged in to ftp.irisa.fr");

            try (var fos = new FileOutputStream(siftArchivePath)) {
                ftpClient.enterLocalPassiveMode();
                Assert.assertTrue(ftpClient.retrieveFile("/local/texmex/corpus/" + siftArchive, fos));
            } finally {
                ftpClient.logout();
                ftpClient.disconnect();
            }

            System.out.printf("%s downloaded%n", siftArchive);
        }
    }

    public static File createTempDir() {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        try {
            if (buildDir != null) {
                return Files.createTempDirectory(Path.of(buildDir), "xodus-test").toFile();
            }

            System.out.println("Build directory is not set !!!");
            return Files.createTempDirectory("xodus-test").toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
