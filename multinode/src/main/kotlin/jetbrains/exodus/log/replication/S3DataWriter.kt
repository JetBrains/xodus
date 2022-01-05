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
package jetbrains.exodus.log.replication

import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.dataStructures.persistent.read
import jetbrains.exodus.core.dataStructures.persistent.write
import jetbrains.exodus.io.AbstractDataWriter
import jetbrains.exodus.io.Block
import jetbrains.exodus.io.RemoveBlockType
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.LogTip
import mu.KLogging
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CopyObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

class S3DataWriter(private val s3Sync: S3Client,
                   override val s3: S3AsyncClient,
                   override val bucket: String,
                   override val requestOverrideConfig: AwsRequestOverrideConfiguration? = null,
                   private val log: Log? = null
) : S3DataReaderOrWriter, AbstractDataWriter() {

    companion object : KLogging()

    private var block: S3FolderBlock? = null

    override val logTip: LogTip? get() = log?.tip

    override fun syncImpl() {}

    override fun write(bytes: ByteArray, off: Int, len: Int): Block {
        with(block ?: throw ExodusException("Can't write, S3DataWriter is closed")) {
            val subBlockAddress = address + length()
            val subBlockSize = len.toLong()
            val key = "${getPartialFolderPrefix(address)}${getPartialFileName(subBlockAddress)}"
            logger.info { "Put file of $key, length: $len" }
            try {
                s3Sync.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .overrideConfiguration(requestOverrideConfig)
                        .key(key)
                        .contentLength(subBlockSize)
                        .build(), RequestBody.fromInputStream(
                        ByteArrayInputStream(bytes, off, len), subBlockSize)
                )
                val blocksCopy = blocks.clone.apply {
                    write {
                        put(subBlockAddress, S3SubBlock(s3factory, subBlockAddress, subBlockSize, address))
                    }
                }
                return S3FolderBlock(s3factory, address, size + subBlockSize, blocksCopy).apply { block = this }
            } catch (e: Exception) {
                val msg = "failed to update '$key' in S3"
                logger.error(msg, e)
                throw ExodusException(msg, e)
            }
        }
    }

    override fun openOrCreateBlockImpl(address: Long, length: Long): Block {
        return newS3FolderBlock(this, address).apply {
            if (length() > length) {
                truncateBlock(address, length)
            }
            block = this
        }
    }

    override fun removeBlock(blockAddress: Long, rbt: RemoveBlockType) {
        val keysToDelete = ArrayList<String>()
        fileBlocks.firstOrNull { it.address == blockAddress }?.apply {
            keysToDelete.add(key)
        }
        if (keysToDelete.isEmpty()) {
            folderBlocks.firstOrNull { it.address == blockAddress }?.apply {
                blocks.read {
                    forEach { keysToDelete.add(it.value.key) }
                }
            }
        }
        if (rbt == RemoveBlockType.Rename) {
            keysToDelete.forEach {
                val newName = it.replace(".xd", ".del")
                logger.debug { "renaming block $it to $newName" }
                try {
                    s3.copyObject(CopyObjectRequest.builder()
                            .bucket(bucket)
                            .overrideConfiguration(requestOverrideConfig)
                            .copySource("$bucket/$it")
                            .key(newName)
                            .build()).get()
                } catch (e: Exception) {
                    val msg = "failed to copy '$it' in S3"
                    logger.error(msg, e)
                    throw ExodusException(msg, e)
                }
            }
        }
        try {
            keysToDelete.deleteS3Objects(s3, bucket, requestOverrideConfig)
        } catch (e: Exception) {
            val msg = "failed to delete files '${keysToDelete.joinToString()}' in S3"
            logger.error(msg, e)
            throw ExodusException(msg, e)
        }
    }

    override fun truncateBlock(blockAddress: Long, length: Long) {
        fileBlocks.firstOrNull { it.address == blockAddress }?.truncate(length)
        // TODO: XD-743
        /*folderBlocks.filter { it.address == blockAddress }.forEach { folder ->
            folder.blocks.let {
                val last = it.findLast { it.address - folder.address < length }
                last?.truncate(length - (last.address - folder.address))
                it.asSequence()
                        .filter { it.address - folder.address >= length }
                        .map { it.key }
                        .windowed(size = deletePackSize, step = deletePackSize, partialWindows = true).forEach {
                            it.deleteS3Objects(s3, bucket, requestOverrideConfig)
                        }
            }
        }*/
    }

    override fun lock(timeout: Long) = true

    override fun release() = true

    override fun lockInfo(): String? = null

    override fun closeImpl() {
        block = null
    }

    override fun clearImpl() {
        listObjects(s3, listObjectsBuilder(bucket, requestOverrideConfig)).filter {
            it.key().isValidAddress || it.key().isValidSubFolder
        }.map {
            it.key()
        }.windowed(size = deletePackSize, step = deletePackSize, partialWindows = true).forEach {
            it.deleteS3Objects(s3, bucket, requestOverrideConfig)
        }
    }

    private fun BasicS3Block.truncate(length: Long) {
        logger.debug { "truncating block at $key to $length" }
        try {
            val array = ByteArray(length.toInt())
            read(array, 0, 0, length.toInt())
            s3.putObject(PutObjectRequest.builder()
                    .bucket(bucket)
                    .overrideConfiguration(requestOverrideConfig)
                    .key(key)
                    .contentLength(length)
                    .build(), object : AsyncRequestBody {

                override fun contentLength() = length

                override fun subscribe(subscriber: Subscriber<in ByteBuffer>) {
                    subscriber.onSubscribe(
                            object : Subscription {
                                override fun request(n: Long) {
                                    if (n > 0) {
                                        subscriber.onNext(ByteBuffer.wrap(array))
                                        subscriber.onComplete()
                                    }
                                }

                                override fun cancel() {}
                            }
                    )
                }
            }).get()
        } catch (e: Exception) {
            val msg = "failed to update $key"
            logger.error(msg, e)
            throw ExodusException(msg, e)
        }
    }

    private fun failIntegrity(): Nothing {
        throw IllegalStateException("Concurrency breach")
    }
}
