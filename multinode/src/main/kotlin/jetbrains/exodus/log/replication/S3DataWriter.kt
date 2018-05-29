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
package jetbrains.exodus.log.replication

import jetbrains.exodus.ExodusException
import jetbrains.exodus.io.AbstractDataWriter
import jetbrains.exodus.log.LogUtil
import mu.KLogging
import software.amazon.awssdk.core.AwsRequestOverrideConfig
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.io.ByteArrayInputStream
import java.util.concurrent.atomic.AtomicReference

class S3DataWriter(val s3: S3AsyncClient,
                   private val s3Sync: S3Client,
                   private val bucketName: String,
                   val requestOverrideConfig: AwsRequestOverrideConfig? = null
) : AbstractDataWriter() {
    companion object : KLogging() {
        internal fun getPartialFileName(address: Long): String {
            return String.format("%016x${LogUtil.LOG_FILE_EXTENSION}", address)
        }

        internal fun getPartialFolderPrefix(blockAddress: Long): String {
            return "_${LogUtil.getLogFilename(blockAddress).replace(LogUtil.LOG_FILE_EXTENSION, "")}/"
        }
    }

    internal val currentFile = AtomicReference<CurrentFile>()

    override fun syncImpl() {
        val file = currentFile.get()
        if (file.length > 0) {
            syncFile(file)
        }
        if (!currentFile.compareAndSet(file, file.copy(position = file.position + file.length, length = 0))) {
            failIntegrity()
        }
    }

    private fun syncFile(file: CurrentFile) {
        val key = "${file.prefix}${getPartialFileName(file.blockAddress + file.position)}"
        try {
            logger.info { "Put file of $key, length: ${file.length}" }
            s3Sync.putObject(PutObjectRequest.builder()
                    .bucket(bucketName)
                    .requestOverrideConfig(requestOverrideConfig)
                    .key(key)
                    .contentLength(file.length.toLong())
                    .build(), RequestBody.of(ByteArrayInputStream(file.buffer, 0, file.length), file.length.toLong()))
            /*s3.putObject(PutObjectRequest.builder()
                    .bucket(bucketName)
                    .requestOverrideConfig(requestOverrideConfig)
                    .key(key)
                    .contentLength(file.length.toLong())
                    .build(), object : AsyncRequestProvider {

                override fun contentLength() = file.length.toLong()

                override fun subscribe(subscriber: Subscriber<in ByteBuffer>) {
                    subscriber.onSubscribe(
                            object : Subscription {
                                override fun request(n: Long) {
                                    if (n > 0) {
                                        subscriber.onNext(ByteBuffer.wrap(file.buffer, 0, file.length))
                                        subscriber.onComplete()
                                    }
                                }

                                override fun cancel() {}
                            }
                    )
                }

            }).get(30, TimeUnit.SECONDS)*/
        } catch (e: Exception) {
            val msg = "failed to update '$key' in S3"
            logger.error(msg, e)
            throw ExodusException(msg, e)
        }
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        val prevFile = currentFile.get()
        // 1. grow
        val grownFile = prevFile.grow(len)
        if (!currentFile.compareAndSet(prevFile, grownFile)) {
            failIntegrity()
        }
        // 2. write
        grownFile.append(b, off, len)
    }

    override fun openOrCreateBlockImpl(address: Long, length: Long) {
        // TODO: this works incorrect for opening existing files
        if (length > Int.MAX_VALUE) {
            throw UnsupportedOperationException("File too large")
        }
        val prevFile = currentFile.get()
        if (!currentFile.compareAndSet(prevFile, CurrentFile(address, length.toInt()))) {
            failIntegrity()
        }
    }

    override fun lock(timeout: Long) = true

    override fun release() = true

    override fun lockInfo(): String? = null

    override fun closeImpl() {
        val file = currentFile.get()
        if (file.length > 0) {
            syncFile(file)
        }
        if (!currentFile.compareAndSet(file, null)) {
            failIntegrity()
        }
    }

    override fun clearImpl() {
        listObjects(s3, listObjectsBuilder(bucketName, requestOverrideConfig)).filter {
            it.key().isValidAddress || it.key().isValidSubFolder
        }.map {
            it.key()
        }.windowed(size = deletePackSize, step = deletePackSize, partialWindows = true).forEach {
            it.deleteS3Objects(s3, bucketName, requestOverrideConfig)
        }
    }

    private fun failIntegrity(): Nothing {
        throw IllegalStateException("Concurrency breach")
    }

    // grow-only buffer suitable for atomic swaps
    // TODO: replace ByteArray with netty ByteBuf
    @Suppress("ArrayInDataClass")
    internal data class CurrentFile(
            val blockAddress: Long,
            val position: Int = 0,
            val length: Int = 0,
            val prefix: String = getPartialFolderPrefix(blockAddress),
            val buffer: ByteArray = ByteArray(1024 * 256) // 0.25 MB by default
    ) {

        fun grow(len: Int): CurrentFile {
            val newLength = length + len
            if (newLength < buffer.size) {
                return copy(length = newLength)
            }
            var newCapacity = length + (length shr 1)
            if (newCapacity < newLength) {
                newCapacity = newLength
            }
            return copy(length = newLength, buffer = buffer.copyOf(newCapacity))
        }

        fun append(b: ByteArray, off: Int, len: Int) {
            System.arraycopy(b, off, buffer, length - len, len)
        }

        fun read(output: ByteArray, position: Long, totalRead: Int, count: Int, offset: Int): Int {
            var result = totalRead
            if (this.position <= position + result) {
                val memoryOffset = (position + result - this.position).toInt()
                if (memoryOffset < length) {
                    val remaining = minOf(count - result, length - memoryOffset)
                    System.arraycopy(buffer, memoryOffset, output, offset + result, remaining)
                    result += remaining
                }
            }
            return result
        }
    }
}
