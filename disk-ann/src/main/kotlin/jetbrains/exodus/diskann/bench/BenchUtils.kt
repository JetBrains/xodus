package jetbrains.exodus.diskann.bench

import jetbrains.exodus.diskann.DiskANN
import jetbrains.exodus.diskann.L2Distance
import jetbrains.exodus.diskann.VectorReader
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.net.ftp.FTP
import org.apache.commons.net.ftp.FTPClient
import java.io.EOFException
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.absolute

object BenchUtils {
    fun runSiftBenchmarks(
        rootDir: Path, siftDir: String, siftArchiveName: String, siftBaseName: String,
        queryFileName: String,
        groundTruthFileName: String, vectorDimensions: Int
    ) {
        println("Working directory: ${rootDir.absolute()}")
        val siftArchivePath = rootDir.resolve(siftArchiveName)
        if (Files.exists(siftArchivePath)) {
            println("$siftArchiveName already exists in $rootDir")
        } else {
            println("Downloading $siftArchiveName from ftp.irisa.fr into $rootDir")

            val ftpClient = FTPClient()
            ftpClient.connect("ftp.irisa.fr")
            ftpClient.enterLocalPassiveMode()
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE)

            val loggedIdn = ftpClient.login("anonymous", "anonymous")
            if (!loggedIdn) {
                throw IllegalStateException("Failed to login to ftp.irisa.fr")
            }

            println("Logged in to ftp.irisa.fr")
            try {

                FileOutputStream(siftArchivePath.toFile()).use { fos ->
                    ftpClient.retrieveFile("/local/texmex/corpus/$siftArchiveName", fos)
                }
            } finally {
                ftpClient.logout()
                ftpClient.disconnect()
            }

            println("$siftArchiveName downloaded")
        }

        println("Extracting $siftArchiveName into $rootDir")

        FileInputStream(rootDir.resolve(siftArchiveName).toFile()).use { fis ->
            GzipCompressorInputStream(fis).use { giz ->
                TarArchiveInputStream(giz).use { tar ->
                    var entry = tar.nextTarEntry

                    while (entry != null) {
                        val name = entry.name
                        if (name.endsWith(".fvecs") || name.endsWith(".ivecs")) {
                            println("Extracting $name")
                            val file = rootDir.resolve(name)
                            if (!Files.exists(file.parent)) {
                                Files.createDirectories(file.parent)
                            }

                            FileOutputStream(file.toFile()).use { fos ->
                                tar.copyTo(fos)
                            }
                        }
                        entry = tar.nextTarEntry
                    }
                }
            }
        }

        println("$siftArchiveName extracted")

        val siftsBaseDir = rootDir.resolve(siftDir)
        val vectors = readFVectors(siftsBaseDir.resolve(siftBaseName), vectorDimensions)

        println("${vectors.size} data vectors loaded with dimension $vectorDimensions, building index...")

        val diskANN = DiskANN("test index", vectorDimensions, L2Distance())
        var ts1 = System.nanoTime()
        diskANN.buildIndex(ArrayVectorReader(vectors))
        var ts2 = System.nanoTime()

        println("Index built in ${(ts2 - ts1) / 1000000} ms")

        println("Reading queries...")
        val queryFile = siftsBaseDir.resolve(queryFileName)
        val queryVectors = readFVectors(queryFile, vectorDimensions)

        println("${queryVectors.size} queries are read")
        println("Reading ground truth...")
        val groundTruthFile = siftsBaseDir.resolve(groundTruthFileName)
        val groundTruth = readIVectors(groundTruthFile, 100)

        println("Ground truth is read, searching...")

        var errorsCount = 0
        ts1 = System.nanoTime()
        for ((index, vector) in queryVectors.withIndex()) {
            val result = diskANN.nearest(vector, 1)
            if (groundTruth[index][0] != result[0].toInt()) {
                errorsCount++
            }
        }
        ts2 = System.nanoTime()
        val errorPercentage = errorsCount * 100.0 / queryVectors.size

        println("Avg. query time : ${(ts2 - ts1) / 1000 / queryVectors.size} us, errors: $errorPercentage%")
    }

    private fun readFVectors(siftBase: Path, vectorDimensions: Int): Array<FloatArray> {
        return FileChannel.open(siftBase).use { siftSmallBaseChannel ->
            val vectorBuffer = ByteBuffer.allocate(Float.SIZE_BYTES * vectorDimensions + Int.SIZE_BYTES)
            vectorBuffer.order(ByteOrder.LITTLE_ENDIAN)

            readFully(siftSmallBaseChannel, vectorBuffer)
            vectorBuffer.rewind()

            val vectorsCount =
                (siftSmallBaseChannel.size() / (Float.SIZE_BYTES * vectorDimensions + Int.SIZE_BYTES)).toInt()
            val vectors = Array(vectorsCount) { FloatArray(vectorDimensions) }

            run {
                val vector = vectors[0]
                for (i in vector.indices) {
                    vector[i] = vectorBuffer.float
                }
            }

            for (i in 1 until vectorsCount) {
                vectorBuffer.clear()
                readFully(siftSmallBaseChannel, vectorBuffer)
                vectorBuffer.rewind()

                vectorBuffer.position(Int.SIZE_BYTES)

                val vector = vectors[i]
                for (j in vector.indices) {
                    vector[j] = vectorBuffer.float
                }
            }
            vectors
        }
    }

    @Suppress("SameParameterValue")
    private fun readIVectors(siftSmallBase: Path, vectorDimensions: Int): Array<IntArray> {
        return FileChannel.open(siftSmallBase).use { siftSmallBaseChannel ->
            val vectorBuffer = ByteBuffer.allocate(Int.SIZE_BYTES * vectorDimensions + Int.SIZE_BYTES)
            vectorBuffer.order(ByteOrder.LITTLE_ENDIAN)

            readFully(siftSmallBaseChannel, vectorBuffer)
            vectorBuffer.rewind()

            val vectorsCount =
                (siftSmallBaseChannel.size() / (Int.SIZE_BYTES * vectorDimensions + Int.SIZE_BYTES)).toInt()
            val vectors = Array(vectorsCount) { IntArray(vectorDimensions) }

            run {
                val vector = vectors[0]
                for (i in vector.indices) {
                    vector[i] = vectorBuffer.int
                }
            }

            for (i in 1 until vectorsCount) {
                vectorBuffer.clear()
                readFully(siftSmallBaseChannel, vectorBuffer)
                vectorBuffer.rewind()

                vectorBuffer.position(Int.SIZE_BYTES)

                val vector = vectors[i]
                for (j in vector.indices) {
                    vector[j] = vectorBuffer.int
                }
            }
            vectors
        }
    }

    private fun readFully(siftSmallBaseChannel: FileChannel, vectorBuffer: ByteBuffer) {
        while (vectorBuffer.remaining() > 0) {
            val r = siftSmallBaseChannel.read(vectorBuffer)
            if (r < 0) {
                throw EOFException()
            }
        }
    }
}

private class ArrayVectorReader(val vectors: Array<FloatArray>) : VectorReader {
    override fun size(): Long {
        return vectors.size.toLong()
    }

    override fun read(index: Long): Pair<Long, FloatArray> {
        return Pair(index, vectors[index.toInt()])
    }
}

