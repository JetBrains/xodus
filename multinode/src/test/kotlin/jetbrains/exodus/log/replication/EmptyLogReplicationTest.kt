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
import org.junit.Test

open class EmptyLogReplicationTest : ReplicationBaseTest() {

    @Test
    fun `should append changes in one file`() {
        var (sourceLog, targetLog) = newLogs()

        val count = 10L
        writeToLog(sourceLog, count)
        val sourceFiles = sourceLog.tip.allFiles
        Assert.assertEquals(1, sourceFiles.size)

        val highAddress = sourceLog.highAddress
        targetLog.appendLog(
                ReplicationDelta(1, 0, highAddress, sourceLog.fileLengthBound, sourceFiles)
        )

        sourceLog.close()

        // check log with cache
        checkLog(targetLog, highAddress, count)

        targetLog = targetLogDir.createLog(fileSize = 4L) {
            cachePageSize = 1024
        }

        // check log without cache
        checkLog(targetLog, highAddress, count)
    }

    @Test
    fun `should append changes in few files`() {
        var (sourceLog, targetLog) = newLogs()

        val count = 1000L
        writeToLog(sourceLog, count)

        val sourceFiles = sourceLog.tip.allFiles
        Assert.assertTrue(sourceFiles.size > 1)

        val highAddress = sourceLog.highAddress
        targetLog.appendLog(
                ReplicationDelta(1, 0, highAddress, sourceLog.fileLengthBound, sourceFiles)
        )

        sourceLog.close()

        // check log with cache
        checkLog(targetLog, highAddress, count)

        targetLog = targetLogDir.createLog(fileSize = 4L) {
            cachePageSize = 1024
        }

        // check log without cache
        checkLog(targetLog, highAddress, count)
    }

}
