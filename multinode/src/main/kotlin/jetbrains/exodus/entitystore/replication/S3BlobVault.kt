/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.replication


import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.core.dataStructures.hash.LongHashMap
import jetbrains.exodus.core.dataStructures.hash.LongSet
import jetbrains.exodus.core.execution.Job
import jetbrains.exodus.entitystore.BlobHandleGenerator
import jetbrains.exodus.entitystore.BlobVault
import jetbrains.exodus.entitystore.BlobVaultItem
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.log.replication.*
import jetbrains.exodus.util.DeferredIO
import mu.KLogging
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Object
import java.io.File
import java.io.InputStream

class S3BlobVault(
        store: PersistentEntityStoreImpl,
        val blobHandleGenerator: BlobHandleGenerator,
        override val s3: S3AsyncClient,
        override val bucket: String,
        val location: String,
        val blobExtension: String,
        override val requestOverrideConfig: AwsRequestOverrideConfiguration? = null
) : BlobVault(store.config), S3FactoryBoilerplate {

    companion object : KLogging()

    override fun getBlob(blobHandle: Long): BlobVaultItem {
        val blobKey = getBlobKey(blobHandle)
        val blob = getS3BlobObject(blobKey)
        if (blob != null) {
            return S3BlobVaultItem(blobHandle, blob)
        }
        return S3MissedBlobVaultItem(blobHandle, blobKey)
    }

    override fun getBlobKey(blobHandle: Long): String {
        return location + super.getBlobKey(blobHandle) + blobExtension
    }

    override fun getBackupStrategy(): BackupStrategy = BackupStrategy.EMPTY

    override fun getContent(blobHandle: Long, txn: Transaction): InputStream? {
        val blobKey = getBlobKey(blobHandle)
        return s3.getObject(
                GetObjectRequest.builder()
                        .overrideConfiguration(requestOverrideConfig)
                        .bucket(bucket)
                        .key(blobKey)
                        .build(),
                AsyncResponseTransformer.toBytes()
        ).get().asInputStream()
    }

    override fun getSize(blobHandle: Long, txn: Transaction): Long {
        val blobKey = getBlobKey(blobHandle)
        return getS3BlobObject(blobKey)?.size() ?: 0
    }


    override fun size() = -1L

    override fun requiresTxn(): Boolean = false

    override fun nextHandle(txn: Transaction): Long {
        return blobHandleGenerator.nextHandle(txn)
    }

    override fun flushBlobs(blobStreams: LongHashMap<InputStream>?, blobFiles: LongHashMap<File>?, deferredBlobsToDelete: LongSet?, txn: Transaction) {
        blobStreams?.let { streams ->
            streams.entries.forEach {
                val key = getBlobKey(it.key)
                s3.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .overrideConfiguration(requestOverrideConfig)
                        .key(key).build(),
                        AsyncRequestBody.fromBytes(it.value.readBytes())
                )
            }
        }
        blobFiles?.let { files ->
            files.entries.forEach {
                val key = getBlobKey(it.key)
                s3.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .overrideConfiguration(requestOverrideConfig)
                        .key(key).build(),
                        AsyncRequestBody.fromFile(it.value)
                )
            }
        }
        deferredBlobsToDelete?.let { blobHandles ->
            val copyHandles = blobHandles.toList()
            val environment = txn.environment
            environment.executeTransactionSafeTask {
                DeferredIO.getJobProcessor().queueIn(object : Job() {
                    override fun execute() {
                        copyHandles.forEach { delete(it) }
                    }

                    override fun getName() = "Delete obsolete blob files"

                    override fun getGroup() = environment.location
                }, environment.environmentConfig.gcFilesDeletionDelay.toLong())
            }
        }
    }

    override fun clear() {
        listObjects(s3, listObjectsBuilder(bucket, requestOverrideConfig)).map {
            it.key()
        }.windowed(size = deletePackSize, step = deletePackSize, partialWindows = true).forEach {
            it.deleteS3Objects(s3, bucket, requestOverrideConfig)
        }
    }

    override fun close() {
        s3.close()
    }

    override fun delete(blobHandle: Long): Boolean {
        val key = getBlobKey(blobHandle)
        return try {
            s3.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .overrideConfiguration(requestOverrideConfig)
                    .key(key).build()).get()
            true
        } catch (e: Exception) {
            logger.warn(e) { "error deleting blob into " }
            false
        }
    }

    private fun getS3BlobObject(blobKey: String): S3Object? {
        val list = listObjects(s3,
                listObjectsBuilder(bucket, requestOverrideConfig).prefix(blobKey)
        ).asIterable()
        return list.firstOrNull()
    }
}