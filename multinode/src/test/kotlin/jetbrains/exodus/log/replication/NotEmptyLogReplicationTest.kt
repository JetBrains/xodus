package jetbrains.exodus.log.replication

import jetbrains.exodus.env.replication.ReplicationDelta
import jetbrains.exodus.util.IOUtil
import org.apache.commons.compress.archivers.zip.ZipFile
import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import java.io.File
import java.io.FileOutputStream


@Ignore
class NotEmptyLogReplicationTest : ReplicationBaseTest() {

    private val db = File(NotEmptyLogReplicationTest::class.java.getResource("/logfiles.zip").toURI())

    @Before
    fun setupLogs() {
        sourceLogDir.also { db.unzipTo(it) }
        targetLogDir.also { db.unzipTo(it) }
    }

    @Test
    fun `should append changes in one file`() {
        var (sourceLog, targetLog) = newLogs()
        val startAddress = sourceLog.highAddress

        val count = 1
        writeToLog(sourceLog, count)

        assertEquals(2, sourceLog.allFileAddresses.size)

        targetLog.appendLog(
                ReplicationDelta(
                        1,
                        startAddress,
                        sourceLog.highAddress,
                        sourceLog.fileSize,
                        sourceLog.filesDelta(startAddress)
                )
        )

        sourceLog.close()

        // check log with cache
        checkLog(targetLog, count, startAddress)

        targetLog = targetLogDir.createLog(fileSize = 4L) {
            cachePageSize = 1024
        }

        // check log without cache
        checkLog(targetLog, count, startAddress)
    }

    @Test
    fun `should append few files to log`() {
        var (sourceLog, targetLog) = newLogs()
        val startAddress = sourceLog.highAddress

        val count = 400
        writeToLog(sourceLog, count)
        Assert.assertTrue(sourceLog.allFileAddresses.size > 1)

        targetLog.appendLog(
                ReplicationDelta(
                        1,
                        startAddress,
                        sourceLog.highAddress,
                        sourceLog.fileSize,
                        sourceLog.filesDelta(startAddress)
                )
        )

        sourceLog.close()

        // check log with cache
        checkLog(targetLog, count, startAddress)

        targetLog = targetLogDir.createLog(fileSize = 4L) {
            cachePageSize = 1024
        }

        // check log without cache
        checkLog(targetLog, count, startAddress)
    }

    @Test(expected = IllegalArgumentException::class)
    fun `should not be able to append with incorrect startAddress`() {
        val (sourceLog, targetLog) = newLogs()

        targetLog.appendLog(
                ReplicationDelta(
                        1,
                        sourceLog.highAddress - 1,
                        sourceLog.highAddress,
                        sourceLog.fileSize,
                        longArrayOf(sourceLog.allFileAddresses.first())
                )
        )
    }

    @Test(expected = IllegalArgumentException::class)
    fun `should not be able to decrease highAddress`() {
        val (sourceLog, targetLog) = newLogs()

        targetLog.appendLog(
                ReplicationDelta(
                        1,
                        sourceLog.highAddress,
                        sourceLog.highAddress - 10,
                        sourceLog.fileSize,
                        longArrayOf(sourceLog.allFileAddresses.first())
                )
        )
    }

    @Test
    fun `should truncate log if incoming files is empty`() {
        val (sourceLog, targetLog) = newLogs()

        targetLog.appendLog(
                ReplicationDelta(
                        1,
                        sourceLog.highAddress,
                        sourceLog.highAddress - 10,
                        sourceLog.fileSize,
                        longArrayOf()
                )
        )

        assertEquals(sourceLog.highAddress - 10, targetLog.highAddress)
    }

    @Test
    fun `should truncate log if incoming files is smaller then start address`() {
        var (sourceLog, targetLog) = newLogs()
        val startAddress = sourceLog.highAddress
        val count = 10
        writeToLog(sourceLog, count)

        assertEquals(2, sourceLog.allFileAddresses.size)

        targetLog.appendLog(
                ReplicationDelta(
                        1,
                        startAddress,
                        sourceLog.highAddress,
                        sourceLog.fileSize,
                        sourceLog.allFileAddresses // smaller then start address
                )
        )

        assertEquals(sourceLog.highAddress, targetLog.highAddress)
        sourceLog.close()

        // check log with cache
        checkLog(targetLog, count, startAddress)

        targetLog = targetLogDir.createLog(fileSize = 4L) {
            cachePageSize = 1024
        }

        // check log without cache
        checkLog(targetLog, count, startAddress)

    }

    private fun File.unzipTo(restoreDir: File) {
        ZipFile(this).use { zipFile ->
            val zipEntries = zipFile.entries
            while (zipEntries.hasMoreElements()) {
                val zipEntry = zipEntries.nextElement()
                val entryFile = File(restoreDir, zipEntry.name)
                if (zipEntry.isDirectory) {
                    entryFile.mkdirs()
                } else {
                    entryFile.parentFile.mkdirs()
                    FileOutputStream(entryFile).use { target -> zipFile.getInputStream(zipEntry).use { `in` -> IOUtil.copyStreams(`in`, target, IOUtil.BUFFER_ALLOCATOR) } }
                }
            }
        }
    }
}
