/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
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

import jetbrains.exodus.bindings.StringBinding.stringToEntry
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.LogConfig
import jetbrains.exodus.log.ReplicatedLogTestMixin
import jetbrains.exodus.util.IOUtil
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.io.File

open class EnvironmentConcurrentAccessTest : ReplicatedLogTestMixin {
    companion object {
        internal const val storeName = "foobar"
        internal const val fileSize = 4L
        internal const val cachePageSize = 1024
        internal val envConfig = EnvironmentConfig().apply {
            isManagementEnabled = false
            logFileSize = fileSize
            logCachePageSize = cachePageSize
            isLogCacheShared = false
        }
    }

    val logDir by lazy { newTmpFile() }

    @Before
    fun setUp() {
        if (logDir.exists()) {
            IOUtil.deleteRecursively(logDir)
        }
    }

    @Test
    fun `should append changes in one file`() {
        doTest(1)
    }

    @Test
    fun `should append changes in longer file`() {
        doTest(5)
    }

    @Test
    fun `should append changes in several files`() {
        doTest(25, 3)
    }

    open fun doTest(multiplier: Int, expectedFiles: Int = 1) {
        val sourceLog = logDir.createLog(releaseLock = true, envConfig = envConfig)

        val sourceEnvironment = Environments.newInstance(sourceLog, envConfig)
        appendEnvironment(sourceEnvironment, 10 * multiplier, 0)

        val targetLog = logDir.createLog(envConfig = envConfig)

        val targetEnvironment = Environments.newInstance(targetLog, envConfig) as EnvironmentImpl

        appendEnvironment(sourceEnvironment, count = 10 * multiplier, from = 10 * multiplier)

        sourceLog.sync()

        checkEnvironment(targetEnvironment, count = 10 * multiplier)

        val sourceFiles = sourceLog.tip.allFiles
        Assert.assertEquals(expectedFiles, sourceFiles.size)

        Assert.assertTrue(targetEnvironment.tryUpdate())

        checkEnvironment(targetEnvironment, count = 20 * multiplier)

        appendEnvironment(sourceEnvironment, count = 10 * multiplier, from = 20 * multiplier)

        sourceEnvironment.close()

        Assert.assertTrue(targetEnvironment.tryUpdate())

        checkEnvironment(targetEnvironment, count = 30 * multiplier)

        targetEnvironment.close()
    }

    protected fun appendEnvironment(env: Environment, count: Int, from: Int) {
        env.executeInTransaction { txn ->
            val store = env.openStore(storeName, StoreConfig.WITHOUT_DUPLICATES, txn)
            ((from + 1)..(from + count)).forEach { i ->
                store.put(txn, stringToEntry("key$i"), stringToEntry("value$i"))
            }
        }
    }

    protected fun checkEnvironment(env: Environment, count: Int) {
        env.executeInTransaction { txn ->
            val store = env.openStore(storeName, StoreConfig.WITHOUT_DUPLICATES, txn)
            Assert.assertEquals(count, store.count(txn).toInt())
        }
    }

    protected fun File.createLog(
            releaseLock: Boolean = false,
            envConfig: EnvironmentConfig
    ): Log {
        return with(LogConfig()) {
            setLocation(this@createLog.canonicalPath)
            Environments.newLogInstance(this, envConfig).also {
                if (releaseLock) { // override locking to perform readonly operations
                    writer.release()
                }
            }
        }
    }
}
