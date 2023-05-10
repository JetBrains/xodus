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

import jetbrains.exodus.TestUtil
import jetbrains.exodus.env.Environments.newInstance
import jetbrains.exodus.io.AsyncFileDataWriter
import jetbrains.exodus.io.FileDataReader
import jetbrains.exodus.log.LogConfig
import jetbrains.exodus.log.LogConfig.Companion.create
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.log.RandomAccessLoggable
import jetbrains.exodus.util.IOUtil.deleteFile
import jetbrains.exodus.util.IOUtil.deleteRecursively
import org.junit.*
import java.io.File
import java.io.IOException
import java.net.MalformedURLException
import java.net.URL
import java.net.URLClassLoader
import java.util.*

open class RecoveryClassLoaderTest {
    @Before
    @Throws(IOException::class)
    fun setUp() {
        testsDirectory[0] = envDirectory
        if (testsDirectory[0]!!.exists()) {
            deleteRecursively(testsDirectory[0]!!)
        } else if (!testsDirectory[0]!!.mkdir()) {
            throw IOException("Failed to create directory for tests.")
        }
    }

    @After
    fun tearDown() {
        deleteRecursively(testsDirectory[0]!!)
        deleteFile(testsDirectory[0]!!)
        testsDirectory[0] = null
    }

    @Test
    @Ignore
    fun testRecovery() {
        runIsolated(OPEN_ENVIRONMENT)
        runIsolated(BREAK_ENVIRONMENT)
        runIsolated(OPEN_ENVIRONMENT)
        runIsolated(CHECK_ENVIRONMENT)
    }

    companion object {
        private var env: Any? = null
        private var cfg: LogConfig? = null

        init {
            runIsolated {}
        }

        private val testsDirectory = arrayOfNulls<File>(1)
        private val OPEN_ENVIRONMENT = Runnable {
            val reader = FileDataReader(testsDirectory[0]!!)
            env = newInstance(create(reader, AsyncFileDataWriter(reader)).also { cfg = it })
        }
        private val BREAK_ENVIRONMENT = Runnable {
            val env = env as Environment?
            val cfg = cfg
            env!!.executeInTransaction { txn: Transaction? ->
                env.openStore(
                    "new_store",
                    StoreConfig.WITHOUT_DUPLICATES,
                    txn!!
                )
            }

            // assertLoggableTypes(log.getLoggablesIterator(0), SEQ);
            env.close()
            val size = cfg!!.getReader()!!.getBlocks(0).iterator().next().length()
            cfg.getWriter()!!.openOrCreateBlock(0, size - 5)
            cfg.getWriter()!!.close()
        }
        private val CHECK_ENVIRONMENT = Runnable {
            val env = env as EnvironmentImpl?
            val log = env!!.log

            // only 'max' first loggables should remain
            // assertLoggableTypes(max, log.getLoggablesIterator(0), SEQ);
            val iter: Iterator<RandomAccessLoggable> = log.getLoggableIterator(0)
            var last: Loggable? = null
            while (iter.hasNext()) {
                last = iter.next()
            }
            Assert.assertNotNull(last)
            Assert.assertEquals(log.getHighAddress(), last!!.end())
        }

        private fun runIsolated(runnable: Runnable) {
            val thread = Thread.currentThread()
            val loader = thread.contextClassLoader
            val classPath = System.getProperty("java.class.path")
            val tokenizer = StringTokenizer(classPath, ":", false)
            try {
                val tokens = ArrayList<URL>()
                while (tokenizer.hasMoreTokens()) {
                    tokens.add(File(tokenizer.nextToken()).toURI().toURL())
                }
                thread.contextClassLoader = object : URLClassLoader(
                    tokens.toTypedArray<URL>(),
                    loader.parent
                ) {
                    @Throws(ClassNotFoundException::class)
                    override fun loadClass(name: String, resolve: Boolean): Class<*>? {
                        // First, check if the class has already been loaded
                        var c = findLoadedClass(name)
                        if (c == null) {
                            c = findClass(name)
                        }
                        if (resolve) {
                            resolveClass(c)
                        }
                        return c
                    }
                }
                runnable.run()
            } catch (e: MalformedURLException) {
                throw RuntimeException(e)
            } finally {
                thread.contextClassLoader = loader
            }
        }

        protected val envDirectory: File
            get() = TestUtil.createTempDir()
    }
}
