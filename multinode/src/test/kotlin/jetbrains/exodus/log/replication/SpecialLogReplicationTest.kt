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
import mu.KLogging
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.io.File

open class SpecialLogReplicationTest : ReplicationBaseTest() {
    companion object : KLogging() {
        val fileSize = 4L // KB
    }

    @Before
    fun setupLogs() {
        sourceLogDir.also { preparedPaddedDB.unzipTo(it) }
    }

    @Test
    fun `should append changes`() {
        var (sourceLog, targetLog) = newLogs()

        targetLog.beginWrite()
        targetLog.padWithNulls()
        targetLog.sync()
        targetLog.endWrite()
        targetLog.close()
        targetLog = targetLogDir.createLog(fileSize) {
            cachePageSize = 1024
        }

        val startAddress = 512L
        targetLog.setHighAddress(targetLog.tip, startAddress)

        Assert.assertEquals(2, sourceLog.tip.allFiles.size)

        val highAddress = sourceLog.highAddress
        targetLog.appendLog(
                ReplicationDelta(
                        1,
                        startAddress,
                        highAddress,
                        sourceLog.fileLengthBound,
                        sourceLog.filesDelta(0)
                )
        )

        sourceLog.close()

        try {
            // check log with cache
            checkLog(targetLog, highAddress, 1000L)
        } catch (t: Throwable) {
            logger.error { "Dump target log directory:" }
            File(targetLog.location).walkTopDown().forEach {
                logger.error { "${it.name} (size = ${it.length()})" }
            }
            throw t
        }

        targetLog = targetLogDir.createLog(fileSize = 4L) {
            cachePageSize = 1024
        }

        // check log without cache
        checkLog(targetLog, highAddress, 1000L)
    }
}
