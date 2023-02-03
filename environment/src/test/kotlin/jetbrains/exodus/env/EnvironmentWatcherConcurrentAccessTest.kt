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
package jetbrains.exodus.env

import jetbrains.exodus.InvalidSettingException
import jetbrains.exodus.io.DataReaderWriterProvider
import org.junit.Assert
import org.junit.Test

class EnvironmentWatcherConcurrentAccessTest : EnvironmentConcurrentAccessTest() {

    companion object {
        private const val timeout = 2000L
        private val envConfigWatch = EnvironmentConfig().apply {
            isManagementEnabled = false
            logFileSize = fileSize
            logCachePageSize = cachePageSize
            isLogCacheShared = false
            logDataReaderWriterProvider = DataReaderWriterProvider.WATCHING_READER_WRITER_PROVIDER
        }
    }

    override fun doTest(multiplier: Int, expectedFiles: Int) {
        val sourceLog = logDir.createLog(envConfig = envConfig)

        val sourceEnvironment = Environments.newInstance(sourceLog, envConfig)
        appendEnvironment(sourceEnvironment, 10 * multiplier, 0)

        val targetLog = logDir.createLog(envConfig = envConfigWatch)

        val targetEnvironment = Environments.newInstance(targetLog, envConfigWatch) as EnvironmentImpl

        var start = targetEnvironment.log.highAddress

        checkEnvironment(targetEnvironment, count = 10 * multiplier)

        appendEnvironment(sourceEnvironment, count = 10 * multiplier, from = 10 * multiplier)

        sourceLog.sync()

        Assert.assertTrue(targetEnvironment.awaitUpdate(start, timeout))

        val sourceFiles = sourceLog.tip.allFiles
        Assert.assertEquals(expectedFiles, sourceFiles.size)

        checkEnvironment(targetEnvironment, count = 20 * multiplier)

        start = targetEnvironment.log.highAddress

        appendEnvironment(sourceEnvironment, count = 10 * multiplier, from = 20 * multiplier)

        sourceEnvironment.close()

        Assert.assertTrue(targetEnvironment.awaitUpdate(start, timeout))

        checkEnvironment(targetEnvironment, count = 30 * multiplier)

        targetEnvironment.close()
    }

    @Test(expected = InvalidSettingException::class)
    fun `unable to set watching environment to read-only mode`() {
        val sourceLog = logDir.createLog(envConfig = envConfig)

        val sourceEnvironment = Environments.newInstance(sourceLog, envConfig)
        appendEnvironment(sourceEnvironment, 10, 0)

        Environments.newInstance(logDir, envConfigWatch).use {
            it.environmentConfig.envIsReadonly = false
        }
    }
}
