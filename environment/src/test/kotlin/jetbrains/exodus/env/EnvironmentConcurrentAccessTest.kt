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
package jetbrains.exodus.env

import jetbrains.exodus.bindings.StringBinding.stringToEntry
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.LogConfig
import jetbrains.exodus.log.ReplicatedLogTestMixin
import jetbrains.exodus.util.IOUtil
import org.junit.Assert
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import java.io.File

class EnvironmentConcurrentAccessTest : ReplicatedLogTestMixin {
    companion object {
        private const val storeName = "foobar"
        private val envConfig = EnvironmentConfig().apply {
            isManagementEnabled = false
        }
    }

    val logDir by lazy { newTmpFile() }

    @Before
    fun setUp() {
        if (logDir.exists()) {
            IOUtil.deleteRecursively(logDir)
        }
    }

    @Ignore // fails with encryption
    @Test
    fun `should append changes in one file`() {
        val sourceLog = logDir.createLog(4L, releaseLock = true, envConfig = envConfig) {
            cachePageSize = 1024
        }

        val sourceEnvironment = Environments.newInstance(sourceLog, envConfig)
        appendEnvironment(sourceEnvironment, 0)

        val targetLog = logDir.createLog(4L, envConfig = envConfig) {
            cachePageSize = 1024
        }

        val targetEnvironment = Environments.newInstance(targetLog, envConfig) as EnvironmentImpl

        appendEnvironment(sourceEnvironment, from = 10)

        sourceLog.sync()

        checkEnvironment(targetEnvironment, count = 10)

        val sourceFiles = sourceLog.tip.allFiles
        Assert.assertEquals(1, sourceFiles.size)

        targetEnvironment.tryUpdate()

        checkEnvironment(targetEnvironment, count = 20)

        appendEnvironment(sourceEnvironment, from = 20)

        sourceEnvironment.close()

        targetEnvironment.tryUpdate()

        checkEnvironment(targetEnvironment, count = 30)

        targetEnvironment.close()
    }

    private fun appendEnvironment(env: Environment, from: Int = 0) {
        env.executeInTransaction { txn ->
            val store = env.openStore(storeName, StoreConfig.WITHOUT_DUPLICATES, txn)
            ((from + 1)..(from + 10)).forEach { i ->
                store.put(txn, stringToEntry("key$i"), stringToEntry("value$i"))
            }
        }
    }

    private fun checkEnvironment(env: Environment, count: Long) {
        env.executeInTransaction { txn ->
            val store = env.openStore(storeName, StoreConfig.WITHOUT_DUPLICATES, txn)
            Assert.assertEquals(count, store.count(txn))
        }
    }

    private fun File.createLog(
            fileSize: Long,
            releaseLock: Boolean = false,
            envConfig: EnvironmentConfig,
            modifyConfig: LogConfig.() -> Unit = {}
    ): Log {
        return with(LogConfig().setFileSize(fileSize)) {
            val (reader, writer) = this@createLog.createLogRW()
            setReaderWriter(reader, writer)
            modifyConfig()
            Environments.newLogInstance(this, envConfig).also {
                if (releaseLock) { // override locking to perform readonly operations
                    writer.release()
                }
            }
        }
    }
}
