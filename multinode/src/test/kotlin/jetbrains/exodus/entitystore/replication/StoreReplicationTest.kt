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
package jetbrains.exodus.entitystore.replication

import jetbrains.exodus.crypto.InvalidCipherParametersException
import jetbrains.exodus.crypto.convert.DirectoryEncryptListenerFactory
import jetbrains.exodus.crypto.convert.ScytaleEngine
import jetbrains.exodus.crypto.convert.encryptBackupable
import jetbrains.exodus.crypto.streamciphers.ChaChaStreamCipherProvider
import jetbrains.exodus.crypto.toBinaryKey
import jetbrains.exodus.entitystore.PersistentEntityStoreConfig
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.entitystore.newPersistentEntityStoreConfig
import jetbrains.exodus.env.EnvironmentConfig
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.newEnvironmentConfig
import jetbrains.exodus.log.replication.ReplicationBaseTest
import org.junit.Assert
import org.junit.Test

open class StoreReplicationTest : ReplicationBaseTest() {

    companion object {

        private val cipherKey = "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f".reversed()
        private const val storeName = "thestore"
        private const val basicIV = 314159262718281828L
        private const val logFileSize = 4L
        private const val logCachePageSize = 1024
        private var port = 8062

        private fun environmentConfigWithMetaServer(port: Int): EnvironmentConfig {
            return newEnvironmentConfig {
                metaServer = MetaServerImpl(port = port)
            }
        }

        private fun environmentConfigEncrypted(): EnvironmentConfig {
            return newEnvironmentConfig {
                cipherId = ChaChaStreamCipherProvider::class.java.name
                setCipherKey(Companion.cipherKey)
                cipherBasicIV = basicIV
                envIsReadonly = true
                logFileSize = Companion.logFileSize
                logCachePageSize = Companion.logCachePageSize
            }
        }

        private fun PersistentEntityStoreImpl.createNIssues(n: Int) {
            executeInTransaction { txn ->
                (1..n).forEach {
                    val issue = txn.newEntity("Issue")
                    issue.setBlobString("description", "Issue with id ${issue.id}")
                }
            }
        }

        private fun PersistentEntityStoreImpl.checkIssues(expectedCount: Int) {
            executeInReadonlyTransaction {
                val allIssues = it.getAll("Issue")
                Assert.assertEquals(expectedCount.toLong(), allIssues.size())
                allIssues.forEach { issue ->
                    Assert.assertEquals("Issue with id ${issue.id}", issue.getBlobString("description"))
                }
            }
        }
    }

    open val lazyBlobs: Boolean get() = false

    @Test
    fun replicate() {
        createEncryptReplicate(port++, cipherKey)
    }

    @Test
    fun replicateEncryptedWithDifferentKey() {
        try {
            createEncryptReplicate(port++, cipherKey.reversed())
            Assert.assertTrue(false)
        } catch (e: InvalidCipherParametersException) {
            // norm
        }
    }

    private fun createEncryptReplicate(port: Int, cipherKey: String) {
        val sourceLog = sourceLogDir.createLog(logFileSize, releaseLock = true) {
            cipherProvider = null
            this.cipherKey = null
            cipherBasicIV = 0
            cachePageSize = 1024
        }

        val sourceConfig = environmentConfigWithMetaServer(port)
        val sourceStore = PersistentEntityStoreImpl(Environments.newInstance(sourceLog, sourceConfig), storeName).apply {
            config.maxInPlaceBlobSize = 0
        }

        val batchCount = 100
        val iterations = 5

        sourceStore.createNIssues(batchCount)

        val output = DirectoryEncryptListenerFactory.newListener(targetLogDir)
        ScytaleEngine(output, ChaChaStreamCipherProvider(), toBinaryKey(cipherKey), basicIV).encryptBackupable(sourceStore)

        val targetConfig = environmentConfigEncrypted()
        val storeConfig = newPersistentEntityStoreConfig {
            storeReplicator = S3Replicator(
                    metaServer = host,
                    metaPort = port,
                    httpClient = httpClient,
                    s3 = s3,
                    bucket = bucket,
                    requestOverrideConfig = extraHost,
                    lazyBlobs = lazyBlobs
            )
        }

        (1..iterations).forEach { i ->

            sourceStore.createNIssues(batchCount)

            PersistentEntityStoreImpl(PersistentEntityStoreConfig.DEFAULT,
                    Environments.newInstance(targetLogDir, targetConfig), null, storeName).use {
                it.checkIssues(batchCount * i)
            }

            PersistentEntityStoreImpl(storeConfig,
                    Environments.newInstance(targetLogDir, targetConfig), null, storeName).use {
                it.checkIssues(batchCount * (i + 1))
            }
        }
    }
}
