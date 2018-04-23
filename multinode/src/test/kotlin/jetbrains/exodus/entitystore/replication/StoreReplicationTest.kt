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
        val cipherKey = "000102030405060708090a0b0c0d0e0f000102030405060708090a0b0c0d0e0f".reversed()
        const val storeName = "thestore"
        const val basicIV = 314159262718281828L
        const val logFileSize = 4L
        const val logCachePageSize = 1024
    }

    @Test
    fun replicate() {
        val sourceLog = sourceLogDir.createLog(logFileSize) {
            cipherProvider = null
            cipherKey = null
            cipherBasicIV = 0
            cachePageSize = 1024
        }

        val environmentConfig = EnvironmentConfig().apply {
            metaServer = MetaServerImpl()
        }
        val sourceStore = PersistentEntityStoreImpl(Environments.newInstance(sourceLog, environmentConfig), storeName)

        sourceStore.executeInTransaction { txn ->
            (1..100).forEach {
                txn.newEntity("Issue")
            }
        }

        val output = DirectoryEncryptListenerFactory.newListener(targetLogDir)
        ScytaleEngine(output, ChaChaStreamCipherProvider(), toBinaryKey(cipherKey), basicIV).encryptBackupable(sourceStore)

        sourceStore.executeInTransaction { txn ->
            (1..100).forEach {
                txn.newEntity("Issue")
            }
        }

        val targetEnvironmentConfig = EnvironmentConfig().also {
            it.cipherId = ChaChaStreamCipherProvider::class.java.name
            it.setCipherKey(cipherKey)
            it.cipherBasicIV = basicIV
            it.envIsReadonly = true
            it.logFileSize = logFileSize
            it.logCachePageSize = logCachePageSize
        }

        var targetStore = PersistentEntityStoreImpl(PersistentEntityStoreConfig(), Environments.newInstance(targetLogDir, targetEnvironmentConfig), null, storeName)

        try {
            targetStore.executeInReadonlyTransaction {
                Assert.assertEquals(100, it.getAll("Issue").size())
            }
        } finally {
            targetStore.close()
        }

        val storeConfig = PersistentEntityStoreConfig().apply {
            storeReplicator = S3Replicator(
                    metaServer = host,
                    httpClient = httpClient,
                    s3 = s3,
                    bucket = bucket,
                    requestOverrideConfig = extraHost
            )
        }

        targetStore = PersistentEntityStoreImpl(storeConfig, Environments.newInstance(targetLogDir, targetEnvironmentConfig), null, storeName)

        targetStore.executeInReadonlyTransaction {
            Assert.assertEquals(200, it.getAll("Issue").size())
        }
    }
}
