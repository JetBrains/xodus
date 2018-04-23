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

import jetbrains.exodus.crypto.convert.DirectoryEncryptListenerFactory
import jetbrains.exodus.crypto.convert.ScytaleEngine
import jetbrains.exodus.crypto.convert.encryptBackupable
import jetbrains.exodus.crypto.streamciphers.ChaChaStreamCipherProvider
import jetbrains.exodus.crypto.toBinaryKey
import jetbrains.exodus.entitystore.PersistentEntityStoreConfig
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.env.EnvironmentConfig
import jetbrains.exodus.env.Environments
import jetbrains.exodus.log.replication.ReplicationBaseTest
import org.junit.Assert
import org.junit.Test

class StoreReplicationTest : ReplicationBaseTest() {

    companion object {

        private val cipherKey = "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f".reversed()
        private const val storeName = "thestore"
        private const val basicIV = 314159262718281828L
        private const val logFileSize = 4L
        private const val logCachePageSize = 1024
        private const val port = 8062

        private fun environmentConfigWithMetaServer(): EnvironmentConfig {
            return EnvironmentConfig().apply {
                metaServer = MetaServerImpl(port = port)
            }
        }

        private fun environmentConfigEncrypted(): EnvironmentConfig {
            return EnvironmentConfig().also {
                it.cipherId = ChaChaStreamCipherProvider::class.java.name
                it.setCipherKey(cipherKey)
                it.cipherBasicIV = basicIV
                it.envIsReadonly = true
                it.logFileSize = logFileSize
                it.logCachePageSize = logCachePageSize
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

        private fun PersistentEntityStoreImpl.checkIssues(expectedCount: Long) {
            executeInReadonlyTransaction {
                val allIssues = it.getAll("Issue")
                Assert.assertEquals(expectedCount, allIssues.size())
                allIssues.forEach { issue ->
                    Assert.assertEquals("Issue with id ${issue.id}", issue.getBlobString("description"))
                }
            }
        }
    }

    @Test
    fun replicate() {
        val sourceLog = sourceLogDir.createLog(logFileSize, releaseLock = true) {
            cipherProvider = null
            cipherKey = null
            cipherBasicIV = 0
            cachePageSize = 1024
        }

        val sourceConfig = environmentConfigWithMetaServer()
        val sourceStore = PersistentEntityStoreImpl(Environments.newInstance(sourceLog, sourceConfig), storeName).apply {
            // config.maxInPlaceBlobSize = 0
        }

        sourceStore.createNIssues(100)

        val output = DirectoryEncryptListenerFactory.newListener(targetLogDir)
        ScytaleEngine(output, ChaChaStreamCipherProvider(), toBinaryKey(cipherKey), basicIV).encryptBackupable(sourceStore)

        sourceStore.createNIssues(100)

        val targetConfig = environmentConfigEncrypted()

        PersistentEntityStoreImpl(PersistentEntityStoreConfig(),
                Environments.newInstance(targetLogDir, targetConfig), null, storeName).use {
            it.checkIssues(100L)
        }

        val storeConfig = PersistentEntityStoreConfig().apply {
            storeReplicator = S3Replicator(
                    metaServer = host,
                    metaPort = port,
                    httpClient = httpClient,
                    s3 = s3,
                    bucket = bucket,
                    requestOverrideConfig = extraHost
            )
        }

        PersistentEntityStoreImpl(storeConfig,
                Environments.newInstance(targetLogDir, targetConfig), null, storeName).use {
            it.checkIssues(200L)
        }
    }
}
