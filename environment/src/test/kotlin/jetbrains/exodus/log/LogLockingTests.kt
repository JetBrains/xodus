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
package jetbrains.exodus.log

import jetbrains.exodus.ExodusException
import org.junit.Assert
import org.junit.Test
import java.io.*

class LogLockingTests : LogTestsBase() {
    @Test
    @Throws(IOException::class)
    fun testLock() {
        initLog(1, 1024)
        val xdLockFile = File(logDirectory, "xd.lck")
        Assert.assertTrue(xdLockFile.exists())
        Assert.assertFalse(canWrite(xdLockFile))
        closeLog()
        Assert.assertTrue(canWrite(xdLockFile))
    }

    @Test
    fun testLockContents() {
        initLog(1, 1024)
        val writer = log.config.getWriter()
        closeLog()
        Assert.assertTrue(writer!!.lockInfo().contains("org.junit."))
    }

    @Test
    fun testDirectoryAlreadyLocked() {
        initLog(1, 1024)
        val xdLockFile = File(logDirectory, "xd.lck")
        Assert.assertTrue(xdLockFile.exists())
        val prevLog = log
        var alreadyLockedEx = false
        try {
            clearLog()
            initLog(1, 1024)
        } catch (ex: ExodusException) {
            alreadyLockedEx = true
        }
        Assert.assertTrue(alreadyLockedEx)
        prevLog.close()
        closeLog()
    }

    @Test
    @Throws(IOException::class)
    fun testDirectoryReleaseLock() {
        initLog(1, 1024)
        var xdLockFile = File(logDirectory, "xd.lck")
        Assert.assertTrue(xdLockFile.exists())
        closeLog()
        xdLockFile = File(logDirectory, "xd.lck")
        val bufferedReader = BufferedReader(FileReader(xdLockFile))
        println(bufferedReader.readLine())
        bufferedReader.close()
        Assert.assertTrue(xdLockFile.exists())
        var alreadyLockedEx = false
        try {
            initLog(1, 1024)
        } catch (ex: ExodusException) {
            alreadyLockedEx = true
        }
        Assert.assertFalse(alreadyLockedEx)
        closeLog()
    }

    companion object {
        @Throws(IOException::class)
        private fun canWrite(xdLockFile: File): Boolean {
            var can = xdLockFile.canWrite()
            if (can) {
                var stream: FileOutputStream? = null
                try {
                    stream = FileOutputStream(xdLockFile)
                    stream.write(42)
                    stream.flush()
                    stream.close()
                } catch (ex: IOException) {
                    // xdLockFile.canWrite() returns true, because of Java cannot recognize tha file is locked
                    can = false
                } finally {
                    if (stream != null) {
                        try {
                            stream.close()
                        } catch (e: IOException) {
                            can = false
                        }
                    }
                }
            }
            // If it didn't help for some reasons (saw that it doesn't work on Solaris 10)
            if (can) {
                try {
                    RandomAccessFile(xdLockFile, "rw").use { file ->
                        file.channel.use { channel ->
                            val lock = channel.tryLock()
                            if (lock != null) {
                                lock.release()
                            } else {
                                can = false
                            }
                            file.close()
                        }
                    }
                } catch (ignore: Throwable) {
                    can = false
                }
            }
            return can
        }
    }
}
