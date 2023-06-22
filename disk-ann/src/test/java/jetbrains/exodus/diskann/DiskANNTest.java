package jetbrains.exodus.diskann;

import it.unimi.dsi.fastutil.longs.LongObjectImmutablePair;
import jetbrains.exodus.TestUtil;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

public class DiskANNTest {
    @Test
    public void testFindLoadedVectors() {
        var vectorDimensions = 64;

        var vectorsCount = 10_000;
        var secureRandom = new SecureRandom();
        var seed = ByteBuffer.wrap(secureRandom.generateSeed(8)).getLong();
        try {
            var rnd = new Random(seed);
            var vectors = new float[vectorsCount][];
            for (var i = 0; i < vectorsCount; i++) {
                var vector = new float[vectorDimensions];
                vectors[i] = vector;
            }

            var addedVectors = new HashSet<FloatArrayHolder>();

            for (float[] vector : vectors) {
                var counter = 0;
                do {
                    if (counter > 0) {
                        System.out.println("duplicate vector found " + counter + ", retrying...");
                    }

                    for (var j = 0; j < vector.length; j++) {
                        vector[j] = 10 * rnd.nextFloat();
                    }
                    counter++;
                } while (!addedVectors.add(new FloatArrayHolder(vector)));
            }

            try (var diskANN = new DiskANN("test index", vectorDimensions, DiskANN.L2_DISTANCE)) {
                var ts1 = System.nanoTime();
                diskANN.buildIndex(new ArrayVectorReader(vectors));
                var ts2 = System.nanoTime();
                System.out.println("Index built in " + (ts2 - ts1) / 1000000 + " ms");

                var errorsCount = 0;
                ts1 = System.nanoTime();
                for (var j = 0; j < vectorsCount; j++) {
                    var vector = vectors[j];
                    var result = diskANN.nearest(vector, 1);
                    Assert.assertEquals("j = $j", 1, result.length);
                    if (j != result[0]) {
                        errorsCount++;
                    }
                }
                ts2 = System.nanoTime();
                var errorPercentage = errorsCount * 100.0 / vectorsCount;

                System.out.printf("Avg. query %d time :  us, errors: %f%%%n", (ts2 - ts1) / 1000 / vectorsCount, errorPercentage);

                Assert.assertTrue(errorPercentage <= 5);

            }

        } catch (Throwable e) {
            System.out.println("Seed: " + seed);
            throw e;
        }
    }

    @Test
    public void testSearchSift10KVectors() throws IOException {
        runSiftBenchmarks(
                "siftsmall", "siftsmall.tar.gz",
                "siftsmall_base.fvecs", "siftsmall_query.fvecs",
                "siftsmall_groundtruth.ivecs", 128
        );

    }

    @SuppressWarnings("SameParameterValue")
    private void runSiftBenchmarks(
            String siftDir, String siftArchive, String siftBaseName,
            String queryFileName, String groundTruthFileName, int vectorDimensions
    ) throws IOException {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!");
        }

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

        var siftSmallDir = TestUtil.createTempDir();
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

        var sifSmallFilesDir = siftSmallDir.toPath().resolve(siftDir);
        var siftSmallBase = sifSmallFilesDir.resolve(siftBaseName);

        var vectors = readFVectors(siftSmallBase.toFile(), vectorDimensions);

        System.out.printf("%d data vectors loaded with dimension %d, building index...",
                vectors.length, vectorDimensions);

        try (var diskANN = new DiskANN("test index", vectorDimensions, DiskANN.L2_DISTANCE)) {
            var ts1 = System.nanoTime();
            diskANN.buildIndex(new ArrayVectorReader(vectors));
            var ts2 = System.nanoTime();

            System.out.printf("Index built in %d ms%n", (ts2 - ts1) / 1000000);

            System.out.println("Reading queries...");
            var queryFile = sifSmallFilesDir.resolve(queryFileName);
            var queryVectors = readFVectors(queryFile.toFile(), vectorDimensions);

            System.out.printf("%d queries are read%n", queryVectors.length);
            System.out.println("Reading ground truth...");

            var groundTruthFile = sifSmallFilesDir.resolve(groundTruthFileName);
            var groundTruth = readIVectors(groundTruthFile.toFile(), 100);
            Assert.assertEquals(queryVectors.length, groundTruth.length);

            System.out.println("Ground truth is read, searching...");

            var errorsCount = 0;
            ts1 = System.nanoTime();
            for (var index = 0; index < queryVectors.length; index++) {
                var vector = queryVectors[index];
                var result = diskANN.nearest(vector, 1);
                Assert.assertEquals("j = " + index, 1, result.length);
                if (groundTruth[index][0] != result[0]) {
                    errorsCount++;
                }
            }
            ts2 = System.nanoTime();
            var errorPercentage = errorsCount * 100.0 / queryVectors.length;

            System.out.printf("Avg. query time : %d us, errors: %f%%%n",
                    (ts2 - ts1) / 1000 / queryVectors.length, errorPercentage);

            Assert.assertTrue(errorPercentage <= 5);
        }

    }

    private float[][] readFVectors(File siftSmallBase, int vectorDimensions) throws IOException {
        try (var siftSmallBaseChannel = FileChannel.open(siftSmallBase.toPath())) {
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
    private int[][] readIVectors(File siftSmallBase, int vectorDimensions) throws IOException {
        try (var siftSmallBaseChannel = FileChannel.open(siftSmallBase.toPath())) {
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

    private void readFully(FileChannel siftSmallBaseChannel, ByteBuffer vectorBuffer) throws IOException {
        while (vectorBuffer.remaining() > 0) {
            var r = siftSmallBaseChannel.read(vectorBuffer);
            Assert.assertTrue(r >= 0);
        }
    }
}

record FloatArrayHolder(float[] floatArray) {

    @Override
    public int hashCode() {
        return Arrays.hashCode(floatArray);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FloatArrayHolder) {
            return Arrays.equals(floatArray, ((FloatArrayHolder) obj).floatArray);
        }
        return false;
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
