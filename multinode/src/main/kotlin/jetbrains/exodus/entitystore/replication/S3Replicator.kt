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
import com.fasterxml.jackson.module.kotlin.KotlinModule
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.entitystore.BlobVault
import jetbrains.exodus.entitystore.FileSystemBlobVaultOld
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.env.replication.EnvironmentAppender
import jetbrains.exodus.env.replication.EnvironmentReplicationDelta
import jetbrains.exodus.env.replication.ReplicationDelta
import jetbrains.exodus.log.replication.FileAsyncHandler
import jetbrains.exodus.log.replication.S3FactoryBoilerplate
import jetbrains.exodus.log.replication.S3FileFactory
import mu.KLogging
import software.amazon.awssdk.core.AwsRequestOverrideConfig
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.services.s3.S3AsyncClient
import java.nio.file.Paths

class S3Replicator(
        val metaServer: String,
        val httpClient: SdkAsyncHttpClient,
        override val s3: S3AsyncClient,
        override val bucket: String,
        override val requestOverrideConfig: AwsRequestOverrideConfig? = null
) : PersistentEntityStoreReplicator, S3FactoryBoilerplate {
    companion object : KLogging() {
        private val objectMapper = ObjectMapper().apply {
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
            registerModule(KotlinModule())
        }
        internal val deltaReader: ObjectReader = objectMapper.readerFor(ReplicationDelta::class.java)
        internal val okReader: ObjectReader = objectMapper.readerFor(MetaServerHandler.OK::class.java)
    }

    override fun replicateEnvironment(environment: Environment): EnvironmentReplicationDelta {
        if (environment !is EnvironmentImpl) {
            throw UnsupportedOperationException("Cannot replicate custom environment")
        }

        val from = environment.log.tip.highAddress

        val result = httpClient.postRequest(
                "/v1/delta/acquire",
                mapOf("fromAddress" to from.toString()),
                metaServer,
                8062
        )

        val delta = deltaReader.readValue<ReplicationDelta>(result, 0, result.size)
        logger.info { "Replication delta acquired: $delta" }

        val factory = S3FileFactory(s3, Paths.get(environment.location), bucket, requestOverrideConfig)

        EnvironmentAppender.appendEnvironment(environment, delta, factory)
        return delta
    }

    override fun replicateBlobVault(delta: EnvironmentReplicationDelta, vault: BlobVault, blobsToReplicate: List<Pair<Long, Long>>) {
        if (vault !is FileSystemBlobVaultOld) {
            throw UnsupportedOperationException("Cannot replicate non-file blob vault")
        }

        blobsToReplicate.forEach {
            val file = vault.getBlobLocation(it.first, false)
            val length = it.second

            try {
                val handler = FileAsyncHandler(
                        file.toPath(),
                        0
                )

                val blobKey = vault.getBlobKey(it.first)
                logger.info { "Copy blob file ${file.path}, key: $blobKey" }
                getRemoteFile(length, 0, blobKey, handler).get().let {
                    if (it.written != length) {
                        throw IllegalStateException("Invalid file, received ${it.written} bytes instead of $length")
                    }
                }
            } catch (t: Throwable) {
                logger.warn(t) { "Cannot replicate file" }
                file.delete()
            }
        }
    }

    override fun endReplication(delta: EnvironmentReplicationDelta) {
        val maybeOk = httpClient.postRequest(
                "/v1/delta/release",
                mapOf("id" to delta.id.toString()),
                metaServer,
                8062
        )
        okReader.readValue<MetaServerHandler.OK>(maybeOk, 0, maybeOk.size).apply {
            if (ok) {
                logger.info { "Replication delta #${delta.id} released" }
            }
        }
    }
}
