/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.log.replication

import jetbrains.exodus.env.replication.ReplicationDelta
import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test

open class NotEmptyLogReplicationTest : ReplicationBaseTest() {
    @Before
    fun setupLogs() {
        sourceLogDir.also { preparedDB.unzipTo(it) }
        targetLogDir.also { preparedDB.unzipTo(it) }
    }

    @Test
    fun `should append changes in one file`() {
        var (sourceLog, targetLog) = newLogs()
        val startAddress = sourceLog.highAddress

        val count = 1L
        val startIndex = 1000L
        writeToLog(sourceLog, count, startIndex)

        assertEquals(2, sourceLog.tip.allFiles.size)

        val highAddress = sourceLog.highAddress
        targetLog.appendLog(
                ReplicationDelta(
                        1,
                        startAddress,
                        highAddress,
                        sourceLog.fileLengthBound,
                        sourceLog.filesDelta(startAddress)
                )
        )

        sourceLog.close()

        // check log with cache
        checkLog(targetLog, highAddress, count, startAddress, startIndex)

        targetLog = targetLogDir.createLog(fileSize = 4L) {
            cachePageSize = 1024
        }

        // check log without cache
        checkLog(targetLog, highAddress, startIndex + count)
    }

    @Test
    fun `should append few files to log`() {
        var (sourceLog, targetLog) = newLogs()
        val startAddress = sourceLog.highAddress

        val count = 400L
        val startIndex = 1000L
        writeToLog(sourceLog, count, startIndex)
        Assert.assertTrue(sourceLog.tip.allFiles.size > 1)

        val highAddress = sourceLog.highAddress
        targetLog.appendLog(
                ReplicationDelta(
                        1,
                        startAddress,
                        highAddress,
                        sourceLog.fileLengthBound,
                        sourceLog.filesDelta(startAddress)
                )
        )

        sourceLog.close()

        // check log with cache
        checkLog(targetLog, highAddress, count, startAddress, startIndex)

        targetLog = targetLogDir.createLog(fileSize = 4L) {
            cachePageSize = 1024
        }

        // check log without cache
        checkLog(targetLog, highAddress, startIndex + count)
    }

    @Test(expected = IllegalArgumentException::class)
    fun `should not be able to append with incorrect startAddress`() {
        val (sourceLog, targetLog) = newLogs()

        targetLog.appendLog(
                ReplicationDelta(
                        1,
                        sourceLog.highAddress - 1,
                        sourceLog.highAddress,
                        sourceLog.fileLengthBound,
                        longArrayOf(sourceLog.tip.allFiles.first())
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
                        sourceLog.fileLengthBound,
                        longArrayOf(sourceLog.tip.allFiles.first())
                )
        )
    }
}
