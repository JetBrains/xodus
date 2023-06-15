package jetbrains.exodus.diskann

import jetbrains.exodus.TestUtil
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.net.ftp.FTP
import org.apache.commons.net.ftp.FTPClient
import org.junit.Assert
import org.junit.Ignore
import org.junit.Test
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.security.SecureRandom
import kotlin.random.Random

class DiskANNTest {
    @Test
    fun testFindLoadedVectors() {
        val vectorDimensions = 64

        val vectorsCount = 10_000
        val secureRandom = SecureRandom()
        val seed = ByteBuffer.wrap(secureRandom.generateSeed(8)).long
        try {
            val rnd = Random(seed)
            val vectors = Array(vectorsCount, { FloatArray(vectorDimensions) })
            val addedVectors = HashSet<FloatArrayHolder>()

            for (i in vectors.indices) {
                val vector = vectors[i]

                var counter = 0
                do {
                    if (counter > 0) {
                        println("duplicate vector found ${counter}, retrying...")
                    }

                    for (j in vector.indices) {
                        vector[j] = 10 * rnd.nextFloat()
                    }
                    counter++
                } while (!addedVectors.add(FloatArrayHolder(vector)))
            }

            val diskANN = DiskANN("test index", vectorDimensions, L2Distance())
            var ts1 = System.nanoTime()
            diskANN.buildIndex(ArrayVectorReader(vectors))
            var ts2 = System.nanoTime()
            println("Index built in ${(ts2 - ts1) / 1000000} ms")

            var errorsCount = 0
            ts1 = System.nanoTime()
            for (j in 0 until vectorsCount) {
                val vector = vectors[j]
                val result = diskANN.nearest(vector, 1)
                Assert.assertEquals("j = $j", 1, result.size)
                if (j.toLong() != result[0]) {
                    errorsCount++
                }
            }
            ts2 = System.nanoTime()
            val errorPercentage = errorsCount * 100.0 / vectorsCount

            println("Avg. query time : ${(ts2 - ts1) / 1000 / vectorsCount} us, errors: $errorPercentage%")

            Assert.assertTrue(errorPercentage <= 5)

        } catch (e: Throwable) {
            println("Seed: $seed")
            throw e
        }
    }

    @Test
    fun testSearchSift10KVectors() {
        runSiftBenchmarks(
            "siftsmall", "siftsmall.tar.gz",
            "siftsmall_base.fvecs", "siftsmall_query.fvecs",
            "siftsmall_groundtruth.ivecs", 128
        )

    }

    @Test
    @Ignore
    fun testSearchSift1MVectors() {
        runSiftBenchmarks(
            "sift", "sift.tar.gz",
            "sift_base.fvecs", "sift_query.fvecs",
            "sift_groundtruth.ivecs", 128
        )
    }

    @Test
    @Ignore
    fun testSearchGist1MVectors() {
        runSiftBenchmarks(
            "gist", "gist.tar.gz",
            "gist_base.fvecs", "gist_query.fvecs",
            "gist_groundtruth.ivecs", 960
        )
    }

    private fun runSiftBenchmarks(
        siftDir: String, siftArchive: String, siftBaseName: String,
        queryFileName: String, groundTruthFileName: String, vectorDimensions: Int
    ) {
        val buildDir = System.getProperty("exodus.tests.buildDirectory")
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!")
        }

        val siftArchivePath = File(buildDir, siftArchive)
        if (siftArchivePath.exists()) {
            println("$siftArchive already exists in ${buildDir}")
        } else {
            println("Downloading $siftArchive from ftp.irisa.fr into ${buildDir}")

            val ftpClient = FTPClient()
            ftpClient.connect("ftp.irisa.fr")
            ftpClient.enterLocalPassiveMode()
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE)

            val loggedIdn = ftpClient.login("anonymous", "anonymous")
            Assert.assertTrue(loggedIdn)
            println("Logged in to ftp.irisa.fr")

            try {

                FileOutputStream(File(File(buildDir), siftArchive)).use { fos ->
                    ftpClient.retrieveFile("/local/texmex/corpus/$siftArchive", fos)
                }
            } finally {
                ftpClient.logout()
                ftpClient.disconnect()
            }

            println("$siftArchive downloaded")
        }

        val siftSmallDir = TestUtil.createTempDir()
        println("Extracting $siftArchive into ${siftSmallDir.absolutePath}")

        FileInputStream(File(buildDir).resolve(siftArchive)).use { fis ->
            GzipCompressorInputStream(fis).use { giz ->
                TarArchiveInputStream(giz).use { tar ->
                    var entry = tar.nextTarEntry

                    while (entry != null) {
                        val name = entry.name
                        if (name.endsWith(".fvecs") || name.endsWith(".ivecs")) {
                            println("Extracting $name")
                            val file = File(siftSmallDir, name)
                            if (!file.parentFile.exists()) {
                                file.parentFile.mkdirs()
                            }

                            FileOutputStream(file).use { fos ->
                                tar.copyTo(fos)
                            }
                        }
                        entry = tar.nextTarEntry
                    }
                }
            }
        }

        println("$siftArchive extracted")

        val sifSmallFilesDir = siftSmallDir.resolve(siftDir)
        val siftSmallBase = sifSmallFilesDir.resolve(siftBaseName)

        val vectors = readFVectors(siftSmallBase, vectorDimensions)

        println("${vectors.size} data vectors loaded with dimension $vectorDimensions, building index...")

        val diskANN = DiskANN("test index", vectorDimensions, L2Distance())
        var ts1 = System.nanoTime()
        diskANN.buildIndex(ArrayVectorReader(vectors))
        var ts2 = System.nanoTime()

        println("Index built in ${(ts2 - ts1) / 1000000} ms")

        println("Reading queries...")
        val queryFile = sifSmallFilesDir.resolve(queryFileName)
        val queryVectors = readFVectors(queryFile, vectorDimensions)

        println("${queryVectors.size} queries are read")
        println("Reading ground truth...")
        val groundTruthFile = sifSmallFilesDir.resolve(groundTruthFileName)
        val groundTruth = readIVectors(groundTruthFile, 100)
        Assert.assertEquals(queryVectors.size, groundTruth.size)

        println("Ground truth is read, searching...")

        var errorsCount = 0
        ts1 = System.nanoTime()
        for ((index, vector) in queryVectors.withIndex()) {
            val result = diskANN.nearest(vector, 1)
            Assert.assertEquals("j = $index", 1, result.size)
            if (groundTruth[index][0] != result[0].toInt()) {
                errorsCount++
            }
        }
        ts2 = System.nanoTime()
        val errorPercentage = errorsCount * 100.0 / queryVectors.size

        println("Avg. query time : ${(ts2 - ts1) / 1000 / queryVectors.size} us, errors: $errorPercentage%")

        Assert.assertTrue(errorPercentage <= 5)
    }

    private fun readFVectors(siftSmallBase: File, vectorDimensions: Int): Array<FloatArray> {
        return FileChannel.open(siftSmallBase.toPath()).use { siftSmallBaseChannel ->
            val vectorBuffer = ByteBuffer.allocate(Float.SIZE_BYTES * vectorDimensions + Int.SIZE_BYTES)
            vectorBuffer.order(ByteOrder.LITTLE_ENDIAN)

            readFully(siftSmallBaseChannel, vectorBuffer)
            vectorBuffer.rewind()

            Assert.assertEquals(vectorDimensions, vectorBuffer.int)

            val vectorsCount =
                (siftSmallBaseChannel.size() / (Float.SIZE_BYTES * vectorDimensions + Int.SIZE_BYTES)).toInt()
            val vectors = Array(vectorsCount, { FloatArray(vectorDimensions) })

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
    private fun readIVectors(siftSmallBase: File, vectorDimensions: Int): Array<IntArray> {
        return FileChannel.open(siftSmallBase.toPath()).use { siftSmallBaseChannel ->
            val vectorBuffer = ByteBuffer.allocate(Int.SIZE_BYTES * vectorDimensions + Int.SIZE_BYTES)
            vectorBuffer.order(ByteOrder.LITTLE_ENDIAN)

            readFully(siftSmallBaseChannel, vectorBuffer)
            vectorBuffer.rewind()

            Assert.assertEquals(vectorDimensions, vectorBuffer.int)

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
            Assert.assertTrue(r >= 0)
        }
    }
}

internal class FloatArrayHolder(val floatArray: FloatArray) {
    override fun equals(other: Any?): Boolean {
        if (other is FloatArrayHolder) {
            return floatArray.contentEquals(other.floatArray)
        }
        return false
    }

    override fun hashCode(): Int {
        return floatArray.contentHashCode()
    }
}

internal class ArrayVectorReader(val vectors: Array<FloatArray>) : VectorReader {
    override fun size(): Long {
        return vectors.size.toLong()
    }

    override fun read(index: Long): Pair<Long, FloatArray> {
        return Pair(index, vectors[index.toInt()])
    }
}