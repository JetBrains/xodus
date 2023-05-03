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

import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.execution.Job
import jetbrains.exodus.core.execution.JobProcessor
import jetbrains.exodus.core.execution.ThreadJobProcessor
import jetbrains.exodus.env.Environments.newInstance
import jetbrains.exodus.io.AsyncFileDataWriter
import jetbrains.exodus.io.FileDataReader
import jetbrains.exodus.log.LogConfig.Companion.create
import org.junit.Assert
import org.junit.Test
import java.io.File

class EnvironmentLockTest : EnvironmentTestsBase() {

    override val envDirectory: File
        get() {
            val dir = super.envDirectory
            return File(dir.parentFile, dir.name)
        }

    @Test
    fun testAlreadyLockedEnvironment() {
        var exOnCreate = false
        var first: Environment? = null
        try {
            val dir = envDirectory
            val reader = FileDataReader(dir)
            first = newInstance(create(reader, AsyncFileDataWriter(reader)), EnvironmentConfig.DEFAULT)
        } catch (ex: ExodusException) {
            //Environment already created on startup!
            exOnCreate = true
        }
        first?.close()
        Assert.assertTrue(exOnCreate)
    }

    private val wasOpened = BooleanArray(1)
    private val processor: JobProcessor = ThreadJobProcessor("EnvironmentLockTest")

    @Test
    @Throws(InterruptedException::class)
    fun testWaitForLockedEnvironment() {
        wasOpened[0] = false
        openConcurrentEnvironment()
        closeEnvironment()
        Thread.sleep(1000)
        Assert.assertTrue(wasOpened[0])
    }

    @Test
    @Throws(InterruptedException::class)
    fun testWaitForLockedEnvironment2() {
        wasOpened[0] = false
        openConcurrentEnvironment()
        Thread.sleep(1000)
        closeEnvironment()
        Thread.sleep(1000)
        Assert.assertTrue(wasOpened[0])
    }

    @Test
    @Throws(InterruptedException::class)
    fun testWaitForLockedEnvironment3() {
        wasOpened[0] = false
        openConcurrentEnvironment()
        Thread.sleep(6000)
        closeEnvironment()
        Thread.sleep(1000)
        Assert.assertFalse(wasOpened[0])
    }

    private fun openConcurrentEnvironment() {
        processor.start()
        object : Job(processor) {
            override fun execute() {
                val dir = envDirectory
                try {
                    val reader = FileDataReader(dir)
                    environment = newEnvironmentInstance(
                        create(reader, AsyncFileDataWriter(reader, LOCK_ID)),
                        EnvironmentConfig().setLogLockTimeout(5000)
                    )
                    wasOpened[0] = true
                } catch (e: ExodusException) {
                    Assert.assertTrue(e.message!!.contains(LOCK_ID))
                    wasOpened[0] = false
                }
            }
        }
    }

    private fun closeEnvironment() {
        if (environment != null) {
            environment!!.close()
            environment = null
        }
    }

    companion object {
        private const val LOCK_ID = "magic 0xDEADBEEF data"
    }
}
