package jetbrains.vectoriadb.bench;

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

public class BenchUtils {
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

                if (vectorBuffer.getInt() != vectorDimensions) {
                    throw new IllegalStateException("Vector dimensions mismatch");
                }

                var vector = new float[vectorDimensions];
                for (var j = 0; j < vector.length; j++) {
                    vector[j] = vectorBuffer.getFloat();
                }
                vectors[i] = vector;
            }
            return vectors;
        }
    }

    public static float[][] readFBVectors(Path path, int vectorDimensions, int size) throws IOException {
        try (var channel = FileChannel.open(path)) {
            var vectorBuffer = ByteBuffer.allocate(vectorDimensions + Integer.BYTES);
            vectorBuffer.order(ByteOrder.LITTLE_ENDIAN);

            var vectorsCount =
                    Math.min(size, (int) (channel.size() / (vectorDimensions + Integer.BYTES)));
            var vectors = new float[vectorsCount][];

            for (var i = 0; i < vectorsCount; i++) {
                vectorBuffer.rewind();
                readFully(channel, vectorBuffer);
                vectorBuffer.rewind();

                if (vectorBuffer.getInt() != vectorDimensions) {
                    throw new IllegalStateException("Vector dimensions mismatch");
                }

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

                if (vectorBuffer.getInt() != vectorDimensions) {
                    throw new IllegalStateException("Vector dimensions mismatch");
                }

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
