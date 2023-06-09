package jetbrains.exodus.log

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.TestUtil
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.io.*
import jetbrains.exodus.tree.ExpiredLoggableCollection
import jetbrains.exodus.util.IOUtil.deleteFile
import jetbrains.exodus.util.IOUtil.deleteRecursively
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.security.SecureRandom
import java.util.*

class RandomLogWriteTest {
    private var log: Log? = null
    private var logDirectory: File? = null
    private var reader: DataReader? = null
    private var writer: DataWriter? = null

    @Before
    @Throws(IOException::class)
    fun setUp() {
        SharedOpenFilesCache.setSize(16)
        if (logDirectory == null) {
            logDirectory = TestUtil.createTempDir()
        }

        if (logDirectory!!.exists()) {
            deleteRecursively(logDirectory!!)
        } else if (!logDirectory!!.mkdir()) {
            throw IOException("Failed to create directory for tests.")
        }
        val logRW: Pair<DataReader, DataWriter> =
            createLogRW()
        reader = logRW.getFirst()
        writer = logRW.getSecond()
    }

    @After
    fun tearDown() {
        closeLog()
        val testsDirectory: File = logDirectory!!

        deleteRecursively(testsDirectory)
        deleteFile(testsDirectory)
        logDirectory = null
    }

    @Test
    fun randomTestSingleBatch() {
        val secureRandom = SecureRandom()

        while (true) {
            val seed = ByteBuffer.wrap(secureRandom.generateSeed(8)).long
            println("seed: $seed")

            val random = Random(seed)
            initLog()

            var iterationsTillEnd = 0
            for (i in 0 until 1_000_000) {
                if (iterationsTillEnd == 0) {
                    log!!.beginWrite()
                    iterationsTillEnd = random.nextInt(1000) + 1
                }


                val size = random.nextInt(4 * 1024 - 37) + 1
                val data = ByteArray(size)

                random.nextBytes(data)
                val dataIterable = ArrayByteIterable(data)
                log!!.write(1, 45, dataIterable, ExpiredLoggableCollection.newInstance(log))

                iterationsTillEnd--
                if (iterationsTillEnd == 0) {
                    log!!.endWrite()
                }
            }

            if (iterationsTillEnd != 0) {
                log!!.endWrite()
            }
            closeLog()
            deleteRecursively(logDirectory!!)
        }

    }

    private fun closeLog() {
        if (log != null) {
            log!!.close()
            log = null
        }
    }


    private fun createLogRW(): Pair<DataReader, DataWriter> {
        val reader = FileDataReader(logDirectory!!)
        return Pair(reader, AsyncFileDataWriter(reader))
    }

    private fun initLog() {
        initLog(LogConfig().setFileSize(4).setCachePageSize(1024))
    }

    private fun initLog(config: LogConfig) {
        if (log == null) {
            log = Log(
                config.setReaderWriter(reader!!, writer!!),
                EnvironmentImpl.CURRENT_FORMAT_VERSION
            )
        }
    }
}