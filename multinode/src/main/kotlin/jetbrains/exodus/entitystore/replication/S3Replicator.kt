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

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.ObjectReader
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.crypto.EncryptedBlobVault
import jetbrains.exodus.entitystore.BlobVault
import jetbrains.exodus.entitystore.DiskBasedBlobVault
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.env.replication.EnvironmentAppender
import jetbrains.exodus.env.replication.EnvironmentReplicationDelta
import jetbrains.exodus.env.replication.ReplicationDelta
import jetbrains.exodus.log.replication.*
import mu.KLogging
import software.amazon.awssdk.core.AwsRequestOverrideConfig
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.services.s3.S3AsyncClient
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.nio.file.Paths

class S3Replicator(
        val metaServer: String,
        val httpClient: SdkAsyncHttpClient,
        private val metaPort: Int = 8062,
        override val s3: S3AsyncClient,
        override val bucket: String,
        override val requestOverrideConfig: AwsRequestOverrideConfig? = null,
        val lazyBlobs: Boolean = false
) : PersistentEntityStoreReplicator, S3FactoryBoilerplate {
    companion object : KLogging() {
        private val objectMapper = ObjectMapper().apply {
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
        }
        internal val deltaReader: ObjectReader = objectMapper.readerFor(ReplicationDelta::class.java)
        internal val okReader: ObjectReader = objectMapper.readerFor(MetaServerHandler.OK::class.java)
    }

    @Volatile
    var sourceEncrypted: Boolean = false

    override fun replicateEnvironment(environment: Environment): EnvironmentReplicationDelta {
        if (environment !is EnvironmentImpl) {
            throw UnsupportedOperationException("Cannot replicate custom environment")
        }

        val from = environment.log.tip.highAddress

        val result = httpClient.postRequest(
                "/v1/delta/acquire",
                mapOf("fromAddress" to from.toString()),
                metaServer,
                metaPort
        )

        val delta = deltaReader.readValue<ReplicationDelta>(result, 0, result.size)
        logger.info { "Replication delta acquired: $delta" }

        sourceEncrypted = delta.encrypted
        val targetEncrypted = environment.cipherProvider != null

        val factory: FileFactory = if (sourceEncrypted == targetEncrypted) {
            S3FileFactory(s3, Paths.get(environment.location), bucket, requestOverrideConfig)
        } else {
            if (!targetEncrypted) {
                throw UnsupportedOperationException("Un-encrypt log is not supported")
            }

            S3ToWriterFileFactory(s3, bucket, requestOverrideConfig) // writer respects encryption
        }

        EnvironmentAppender.appendEnvironment(environment, delta, factory)
        return delta
    }

    override fun replicateBlobVault(delta: EnvironmentReplicationDelta, vault: BlobVault, blobsToReplicate: List<Pair<Long, Long>>) {
        if (lazyBlobs) {
            if (logger.isInfoEnabled) {
                logger.info("Blob vault ${vault.javaClass.simpleName} will be replicated in a lazy manner")
            }
            return
        }

        if (logger.isInfoEnabled) {
            logger.info("Will replicate " + blobsToReplicate.size + " blobs")
        }

        if (vault !is DiskBasedBlobVault) {
            throw UnsupportedOperationException("Cannot replicate non-file blob vault")
        }

        val targetEncrypted = vault is EncryptedBlobVault

        blobsToReplicate.forEach {
            replicateBlob(it.first, it.second, vault, sourceEncrypted, targetEncrypted)
        }
    }

    override fun decorateBlobVault(vault: DiskBasedBlobVault, store: PersistentEntityStore): DiskBasedBlobVault {
        return if (lazyBlobs) {
            S3BlobVault(vault, store as PersistentEntityStoreImpl, this)
        } else {
            vault
        }
    }

    override fun endReplication(delta: EnvironmentReplicationDelta) {
        val maybeOk = httpClient.postRequest(
                "/v1/delta/release",
                mapOf("id" to delta.id.toString()),
                metaServer,
                metaPort
        )
        okReader.readValue<MetaServerHandler.OK>(maybeOk, 0, maybeOk.size).apply {
            if (ok) {
                logger.info { "Replication delta #${delta.id} released" }
            }
        }
    }

    internal fun replicateBlob(handle: Long, length: Long, vault: DiskBasedBlobVault, sourceEncrypted: Boolean, targetEncrypted: Boolean): File? {
        val blobKey = vault.getBlobKey(handle)
        val file = vault.getBlobLocation(handle, false)
        logger.debug { "Copy blob file ${file.path}, key: $blobKey" }

        try {
            if (sourceEncrypted == targetEncrypted) {
                getRemoteFile(
                        length = length,
                        startingLength = 0,
                        name = blobKey,
                        handler = FileAsyncHandler(path = file.toPath(), startingLength = 0)
                ).get().written
            } else {
                if (vault !is EncryptedBlobVault) {
                    throw UnsupportedOperationException("Un-encrypt blobs is not supported")
                }

                val fileStream = FileOutputStream(file)
                val stream = vault.wrapOutputStream(handle, BufferedOutputStream(fileStream))

                val handler = BufferQueueAsyncHandler()

                val request = getRemoteFile(length = length, startingLength = 0, name = blobKey, handler = handler)
                val queue = handler.queue
                val subscription = handler.subscription

                var written = 0L

                while (true) {
                    val buffer = queue.take()
                    if (buffer === BufferQueueAsyncHandler.finish) {
                        break
                    }
                    val count = buffer.remaining()
                    val output = ByteArray(count)
                    buffer.get(output)
                    subscription.request(1)
                    stream.write(output)
                    written += count
                }

                stream.flush()
                fileStream.fd.sync()

                request.get().contentLength()
            }.let {
                if (it != length) {
                    throw IllegalStateException("Invalid file, received $it bytes instead of $length")
                }
            }
        } catch (t: Throwable) {
            logger.warn(t) { "Cannot replicate file" }
            file.delete()
            return null
        }

        return file
    }
}
