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
package jetbrains.exodus.entitystore

import jetbrains.exodus.TestUtil
import jetbrains.exodus.backup.BackupBean
import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.backup.VirtualFileDescriptor
import jetbrains.exodus.core.execution.Job
import jetbrains.exodus.core.execution.JobProcessorExceptionHandler
import jetbrains.exodus.core.execution.ThreadJobProcessor
import jetbrains.exodus.util.CompressBackupUtil
import jetbrains.exodus.util.IOUtil
import jetbrains.exodus.util.Random
import junit.framework.TestCase
import org.apache.commons.compress.archivers.zip.ZipFile
import java.io.File
import java.io.FileOutputStream
import java.io.IOException

class BackupTests : EntityStoreTestBase() {

    override fun needsImplicitTxn(): Boolean {
        return false
    }

    @Throws(Exception::class)
    fun testSingular() {
        val store = entityStore
        store.config.maxInPlaceBlobSize = 0 // no in-place blobs
        val randomDescription = arrayOfNulls<String>(1)
        store.executeInTransaction { txn ->
            val issue = txn.newEntity("Issue")
            randomDescription[0] = java.lang.Double.toString(Math.random())
            issue.setBlobString("description", randomDescription[0]!!)
        }
        val backupDir = TestUtil.createTempDir()
        try {
            val backup = CompressBackupUtil.backup(store, backupDir, null, true)
            val restoreDir = TestUtil.createTempDir()
            try {
                extractEntireZip(backup, restoreDir)
                val newStore = PersistentEntityStores.newInstance(restoreDir)
                newStore.use { _ ->
                    newStore.executeInReadonlyTransaction { txn ->
                        assertEquals(1, txn.getAll("Issue").size())
                        val issue = txn.getAll("Issue").first
                        assertNotNull(issue)
                        assertEquals(randomDescription[0], issue!!.getBlobString("description"))
                    }
                }
            } finally {
                IOUtil.deleteRecursively(restoreDir)
            }
        } finally {
            IOUtil.deleteRecursively(backupDir)
        }
    }

    @Throws(Exception::class)
    fun testStress() {
        doStressTest(false)
    }

    @Throws(Exception::class)
    fun testStressWithBackupBean() {
        doStressTest(true)
    }

    @Throws(Exception::class)
    fun testInterruptedIsDeleted() {
        testSingular()
        val backupDir = TestUtil.createTempDir()
        try {
            val storeBackupStrategy = entityStore.backupStrategy
            val backup = CompressBackupUtil.backup({
                object : BackupStrategy() {
                    @Throws(Exception::class)
                    override fun beforeBackup() {
                        storeBackupStrategy.beforeBackup()
                    }

                    override fun getContents(): Iterable<VirtualFileDescriptor> {
                        return storeBackupStrategy.contents
                    }

                    @Throws(Exception::class)
                    override fun afterBackup() {
                        storeBackupStrategy.afterBackup()
                    }

                    override fun isInterrupted(): Boolean {
                        return true
                    }

                    override fun acceptFile(file: VirtualFileDescriptor): Long {
                        return storeBackupStrategy.acceptFile(file)
                    }
                }
            }, backupDir, null, true)
            assertFalse(backup.exists())
        } finally {
            IOUtil.deleteRecursively(backupDir)
        }
    }

    @Throws(Exception::class)
    fun doStressTest(useBackupBean: Boolean) {
        val store = entityStore
        store.config.maxInPlaceBlobSize = 0 // no in-place blobs
        val issueCount = 1000
        store.executeInTransaction { txn ->
            for (i in 0 until issueCount) {
                val issue = txn.newEntity("Issue")
                issue.setBlobString("description", java.lang.Double.toString(Math.random()))
            }
        }
        val rnd = Random()
        val finish = booleanArrayOf(false)
        val backgroundChanges = intArrayOf(0)
        val threadCount = 4
        val threads = arrayOfNulls<ThreadJobProcessor>(threadCount)
        for (i in threads.indices) {
            threads[i] = ThreadJobProcessor("BackupTest Job Processor $i").apply {
                start()
                exceptionHandler = JobProcessorExceptionHandler { _, _, t -> println(t.toString()) }
                queue(object : Job() {
                    @Throws(Throwable::class)
                    override fun execute() {
                        while (!finish[0]) {
                            store.executeInTransaction { txn ->
                                val issue = txn.getAll("Issue").skip(rnd.nextInt(issueCount - 1)).first
                                TestCase.assertNotNull(issue)
                                issue!!.setBlobString("description", java.lang.Double.toString(Math.random()))
                                print("\r" + ++backgroundChanges[0])
                            }
                        }
                    }
                })
            }
        }
        Thread.sleep(1000)
        val backupDir = TestUtil.createTempDir()
        try {
            val backup = CompressBackupUtil.backup(if (useBackupBean) BackupBean(store) else store, backupDir, null, true)
            finish[0] = true
            val restoreDir = TestUtil.createTempDir()
            try {
                extractEntireZip(backup, restoreDir)
                val newStore = PersistentEntityStores.newInstance(restoreDir)
                newStore.use { _ ->
                    val lastUsedBlobHandle = longArrayOf(-1L)
                    newStore.executeInReadonlyTransaction { t ->
                        val txn = t as PersistentStoreTransaction
                        TestCase.assertEquals(issueCount.toLong(), txn.getAll("Issue").size())
                        lastUsedBlobHandle[0] = newStore.getSequence(txn, PersistentEntityStoreImpl.BLOB_HANDLES_SEQUENCE).loadValue(txn)
                        for (issue in txn.getAll("Issue")) {
                            val description = issue.getBlobString("description")
                            TestCase.assertNotNull(description)
                            TestCase.assertFalse(description!!.isEmpty())
                        }
                    }
                    val blobVault = newStore.blobVault.sourceVault as FileSystemBlobVault
                    for (fd in blobVault.backupStrategy.contents) {
                        val file = (fd as BackupStrategy.FileDescriptor).file
                        if (file.isFile && file.name != FileSystemBlobVaultOld.VERSION_FILE) {
                            TestCase.assertTrue("" + blobVault.getBlobHandleByFile(file) + " > " + lastUsedBlobHandle[0],
                                    blobVault.getBlobHandleByFile(file) <= lastUsedBlobHandle[0])
                        }
                    }
                }
            } finally {
                IOUtil.deleteRecursively(restoreDir)
            }
        } finally {
            IOUtil.deleteRecursively(backupDir)
        }
        for (thread in threads) {
            thread?.finish()
        }
    }

    companion object {

        @Throws(IOException::class)
        fun extractEntireZip(zip: File, restoreDir: File) {
            ZipFile(zip).use { zipFile ->
                val zipEntries = zipFile.entries
                while (zipEntries.hasMoreElements()) {
                    val zipEntry = zipEntries.nextElement()
                    val entryFile = File(restoreDir, zipEntry.name)
                    if (zipEntry.isDirectory) {
                        entryFile.mkdirs()
                    } else {
                        entryFile.parentFile.mkdirs()
                        FileOutputStream(entryFile).use { target -> zipFile.getInputStream(zipEntry).use { `in` -> IOUtil.copyStreams(`in`, target, IOUtil.BUFFER_ALLOCATOR) } }
                    }
                }
            }
        }
    }

}
