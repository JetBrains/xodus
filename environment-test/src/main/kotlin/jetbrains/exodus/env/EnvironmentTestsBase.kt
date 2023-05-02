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

import jetbrains.exodus.*
import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.core.execution.Job
import jetbrains.exodus.core.execution.JobProcessor
import jetbrains.exodus.core.execution.ThreadJobProcessor
import jetbrains.exodus.core.execution.locks.Latch
import jetbrains.exodus.env.*
import jetbrains.exodus.env.Environments.newContextualInstance
import jetbrains.exodus.env.Environments.newInstance
import jetbrains.exodus.io.*
import jetbrains.exodus.io.SharedOpenFilesCache.Companion.invalidate
import jetbrains.exodus.log.*
import jetbrains.exodus.log.Log.Companion.invalidateSharedCache
import jetbrains.exodus.log.LogConfig.Companion.create
import jetbrains.exodus.util.CompressBackupUtil.archiveFile
import jetbrains.exodus.util.IOUtil.deleteFile
import jetbrains.exodus.util.IOUtil.deleteRecursively
import jetbrains.exodus.util.IOUtil.listFiles
import jetbrains.exodus.TestUtil
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.junit.After
import org.junit.Assert
import org.junit.Before
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes
import java.util.zip.GZIPOutputStream
import jetbrains.exodus.core.dataStructures.Pair

open class EnvironmentTestsBase {
    protected var environment: EnvironmentImpl? = null
    private var processor: JobProcessor? = null

    private var _envDirectory: File? = null
    protected open val envDirectory: File
        get() {
            if (_envDirectory == null) {
                _envDirectory = TestUtil.createTempDir()
            }
            return _envDirectory!!
        }

    protected var reader: DataReader? = null
    protected var writer: DataWriter? = null

    @Before
    @Throws(Exception::class)
    open fun setUp() {
        invalidateSharedCaches()
        val readerWriterPair = createRW()
        reader = readerWriterPair.first
        writer = readerWriterPair.second
        createEnvironment()
        processor = ThreadJobProcessor("EnvironmentTestsBase processor")
        processor!!.start()
    }

    @After
    @Throws(Exception::class)
    open fun tearDown() {
        try {
            if (environment != null) {
                environment!!.close()
                environment = null
            }
        } catch (e: ExodusException) {
            archiveDB(javaClass.name + '.' + System.currentTimeMillis() + ".tar.gz")
            throw e
        } finally {
            invalidateSharedCaches()
            deleteRW()
            if (processor != null) {
                processor!!.finish()
            }
        }
    }

    private fun archiveDB(target: String) {
        archiveDB(environment!!.location, target)
    }

    protected fun newContextualEnvironmentInstance(
        config: LogConfig?,
        ec: EnvironmentConfig? = EnvironmentConfig()
    ): EnvironmentImpl {
        return newContextualInstance(config!!, ec!!) as EnvironmentImpl
    }

    protected open fun createEnvironment() {
        environment = newEnvironmentInstance(create(reader!!, writer!!))
    }

    protected val log: Log
        get() = environment!!.log

    @Throws(IOException::class)
    protected open fun createRW(): Pair<DataReader, DataWriter> {
        val testsDirectory = envDirectory

        if (testsDirectory.exists()) {
            deleteRecursively(testsDirectory)
        } else if (!testsDirectory.mkdir()) {
            throw IOException("Failed to create directory for tests.")
        }
        val reader = FileDataReader(testsDirectory)
        return Pair(reader, AsyncFileDataWriter(reader))
    }

    protected open fun deleteRW() {
        val testsDirectory = _envDirectory
        if (testsDirectory != null) {
            deleteRecursively(testsDirectory)
            deleteFile(testsDirectory)
            _envDirectory = null
        }

        reader = null
        writer = null
    }


    protected fun executeParallelTransaction(runnable: TransactionalExecutable) {
        try {
            val sync = Latch.create()
            sync.acquire()
            processor!!.queue(object : Job() {
                override fun execute() {
                    environment!!.executeInTransaction(runnable)
                    sync.release()
                }
            })
            sync.acquire()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            // ignore
        }
    }

    protected fun runParallelRunnable(runnable: Runnable) {
        processor!!.queue(object : Job() {
            override fun execute() {
                runnable.run()
            }
        })
    }

    protected fun openStoreAutoCommit(name: String, config: StoreConfig?): Store {
        return environment!!.computeInTransaction { txn: Transaction ->
            environment!!.openStore(
                name, config!!, txn
            )
        }
    }

    protected fun putAutoCommit(
        store: Store,
        key: ByteIterable,
        value: ByteIterable
    ) {
        environment!!.executeInTransaction { txn: Transaction ->
            store.put(
                txn, key, value
            )
        }
    }

    protected fun getAutoCommit(
        store: Store,
        key: ByteIterable
    ): ByteIterable? {
        return environment!!.computeInReadonlyTransaction { txn: Transaction -> store[txn, key] }
    }

    protected fun deleteAutoCommit(
        store: Store,
        key: ByteIterable
    ): Boolean {
        return environment!!.computeInTransaction { txn: Transaction ->
            store.delete(
                txn, key
            )
        }
    }

    protected fun countAutoCommit(store: Store): Long {
        return environment!!.computeInTransaction { txn: Transaction ->
            store.count(
                txn
            )
        }
    }

    protected fun assertNotNullStringValue(
        store: Store,
        keyEntry: ByteIterable?,
        value: String?
    ) {
        environment!!.executeInTransaction { txn: Transaction ->
            assertNotNullStringValue(
                txn,
                store,
                keyEntry,
                value
            )
        }
    }

    protected fun assertNotNullStringValue(
        txn: Transaction?, store: Store,
        keyEntry: ByteIterable?, value: String?
    ) {
        val valueEntry = store[txn!!, keyEntry!!]
        Assert.assertNotNull(valueEntry)
        Assert.assertEquals(value, StringBinding.entryToString(valueEntry!!))
    }

    protected fun assertEmptyValue(store: Store, keyEntry: ByteIterable?) {
        environment!!.executeInTransaction { txn: Transaction? -> assertEmptyValue(txn, store, keyEntry) }
    }

    protected fun assertEmptyValue(txn: Transaction?, store: Store, keyEntry: ByteIterable?) {
        val valueEntry = store[txn!!, keyEntry!!]
        Assert.assertNull(valueEntry)
    }

    @Suppress("SameParameterValue")
    protected fun assertNotNullStringValues(
        store: Store,
        vararg values: String?
    ) {
        environment!!.executeInTransaction { txn: Transaction? ->
            store.openCursor(
                txn!!
            ).use { cursor ->
                var i = 0
                while (cursor.next) {
                    val valueEntry = cursor.value
                    Assert.assertNotNull(valueEntry)
                    val value = values[i++]
                    Assert.assertEquals(value, StringBinding.entryToString(valueEntry))
                }
            }
        }
    }

    protected fun reopenEnvironment() {
        val envConfig = environment!!.environmentConfig
        environment!!.close()
        environment = newEnvironmentInstance(create(reader!!, writer!!), envConfig)
    }

    protected fun setLogFileSize(kilobytes: Int) {
        val environmentConfig = EnvironmentConfig().setLogCacheShared(false)
        if (environmentConfig.logFileSize != kilobytes.toLong()) {
            val envConfig = environment!!.environmentConfig
            if (envConfig.logCachePageSize > kilobytes * 1024) {
                envConfig.setLogCachePageSize(kilobytes * 1024)
            }
            envConfig.setLogFileSize(kilobytes.toLong())
            recreateEnvinronment(envConfig)
        }
    }

    protected fun recreateEnvinronment(envConfig: EnvironmentConfig?) {
        environment!!.close()
        invalidateSharedCaches()
        deleteRW()
        val readerWriterPair: Pair<DataReader, DataWriter> = try {
            createRW()
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
        reader = readerWriterPair.first
        writer = readerWriterPair.second
        val logConfig = create(reader!!, writer!!)
        environment = newEnvironmentInstance(logConfig, envConfig)
    }

    protected fun set1KbFileWithoutGC() {
        setLogFileSize(1)
        environment!!.environmentConfig.setGcEnabled(false)
    }

    protected fun set2KbFileWithoutGC() {
        setLogFileSize(2)
        environment!!.environmentConfig.setGcEnabled(false)
    }

    private fun invalidateSharedCaches() {
        invalidateSharedCache()
        try {
            invalidate()
        } catch (ignore: IOException) {
        }
    }

    companion object {
        fun archiveDB(location: String, target: String) {
            try {
                println("Dumping $location to $target")
                val root = File(location)
                val targetFile = File(target)
                val tarGz = TarArchiveOutputStream(
                    GZIPOutputStream(
                        BufferedOutputStream(FileOutputStream(targetFile)), 0x1000
                    )
                )
                for (file in listFiles(root)) {
                    val basicFileAttributes = Files.readAttributes(file.toPath(), BasicFileAttributes::class.java)
                    val fileSize = basicFileAttributes.size()
                    if (basicFileAttributes.isRegularFile && fileSize != 0L) {
                        archiveFile(tarGz, BackupStrategy.FileDescriptor(file, ""), fileSize)
                    }
                }
                tarGz.close()
            } catch (ioe: IOException) {
                println("Can't create backup")
            }
        }

        fun newEnvironmentInstance(config: LogConfig?): EnvironmentImpl {
            return newInstance(config!!, EnvironmentConfig().setLogCacheShared(false)) as EnvironmentImpl
        }

        fun newEnvironmentInstance(config: LogConfig?, ec: EnvironmentConfig?): EnvironmentImpl {
            return newInstance(config!!, ec!!) as EnvironmentImpl
        }

        fun assertLoggableTypes(log: Log?, address: Int, vararg types: Int) {
            assertLoggableTypes(Int.MAX_VALUE, log!!.getLoggableIterator(address.toLong()), *types)
        }

        private fun assertLoggableTypes(@Suppress("SameParameterValue") max: Int,
                                        it: Iterator<RandomAccessLoggable>, vararg types: Int) {
            for ((i, type) in types.withIndex()) {
                if (i + 1 > max) {
                    break
                }
                while (true) {
                    Assert.assertTrue(it.hasNext())
                    val loggable = it.next()
                    if (loggable.type == HashCodeLoggable.TYPE) {
                        continue
                    }
                    Assert.assertEquals(type.toLong(), loggable.type.toLong())
                    break
                }
            }
            while (it.hasNext()) {
                val loggable = it.next()
                if (loggable.type == HashCodeLoggable.TYPE) {
                    continue
                }
                Assert.fail()
            }
        }
    }
}
