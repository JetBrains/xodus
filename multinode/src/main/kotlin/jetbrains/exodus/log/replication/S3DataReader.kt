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
import jetbrains.exodus.io.Block
import jetbrains.exodus.io.DataReader
import jetbrains.exodus.io.RemoveBlockType
import jetbrains.exodus.log.LogUtil
import mu.KLogging
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import software.amazon.awssdk.core.AwsRequestOverrideConfig
import software.amazon.awssdk.core.async.AsyncRequestProvider
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.*
import java.nio.ByteBuffer
import java.util.*
import kotlin.Comparator
import kotlin.collections.ArrayList


class S3DataReader(
        val s3: S3AsyncClient,
        val bucketName: String,
        val requestOverrideConfig: AwsRequestOverrideConfig? = null) : DataReader {

    companion object : KLogging() {
        private const val logFileNameWithExtLength = 19

        private fun decodeAddress(logFilename: String): Long {
            if (!checkAddress(logFilename)) {
                throw ExodusException("Invalid log file name: $logFilename")
            }
            return logFilename.substring(0, 16).toLong(16)
        }

        private fun checkAddress(logFilename: String): Boolean {
            return logFilename.length == logFileNameWithExtLength && logFilename.endsWith(LogUtil.LOG_FILE_EXTENSION)
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun getBlocks(): Iterable<Block> {
        val result = TreeSet<Block>(Comparator<Block> { o1, o2 ->
            o1.address.compareTo(o2.address)
        })
        return result.apply {
            addAll(fileBlocks)
            addAll(folderBlocks)
        }
    }

    override fun getBlocks(fromAddress: Long): Iterable<Block> = blocks.filter { it.address >= fromAddress }.toList()

    override fun getLocation(): String = "s3:$bucketName"

    override fun removeBlock(blockAddress: Long, rbt: RemoveBlockType) {
        val keysToDelete = ArrayList<String>()

        fileBlocks.filter { it.address == blockAddress }.forEach {
            keysToDelete.add(it.s3Object.key())
        }

        folderBlocks.filter { it.address == blockAddress }.flatMap { it.blocks }.forEach {
            keysToDelete.add(it.s3Object.key())
        }
        if (rbt == RemoveBlockType.Rename) {
            keysToDelete.forEach {
                val newName = it.replace(".xd", ".del")
                logger.debug { "renaming block $it to $newName" }

                try {
                    s3.copyObject(CopyObjectRequest.builder()
                            .bucket(bucketName)
                            .requestOverrideConfig(requestOverrideConfig)
                            .copySource("$bucketName/$it")
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
            keysToDelete.deleteS3Objects()
        } catch (e: Exception) {
            val msg = "failed to delete files '${keysToDelete.joinToString()}' in S3"
            logger.error(msg, e)
            throw ExodusException(msg, e)
        }
    }

    override fun truncateBlock(blockAddress: Long, length: Long) {
        fileBlocks.filter { it.address == blockAddress }.forEach { it.truncate(length) }

        folderBlocks.filter { it.address == blockAddress }.forEach { folder ->
            folder.blocks.let {
                val last = it.findLast { it.address - folder.address < length }
                last?.truncate(length - (last.address - folder.address))
                it.filter { it.address - folder.address >= length }.map { it.s3Object.key() }.deleteS3Objects()
            }
        }
    }

    override fun close() = s3.close()

    override fun getBlock(address: Long): Block {
        logger.debug { "Get block at ${LogUtil.getLogFilename(address)}" }
        return blocks.firstOrNull { it.address == address } ?: NoBlock(address)
    }

    override fun clear() {
        val builder = listObjectsBuilder()
        while (true) {
            val response = s3.listObjects(builder.build()).get()
            response.contents()?.let {
                it.filter { it.key().isValidAddress || it.key().isValidSubFolder }
                        .map { it.key() }.deleteS3Objects()
            }
            if (!response.isTruncated) {
                break
            }
            builder.marker(response.marker())
        }
    }

    internal inner class S3Block(val _address: Long, internal val s3Object: S3Object) : Block {

        override fun getAddress() = _address

        override fun setReadOnly() = true

        override fun length(): Long = s3Object.size()

        override fun read(output: ByteArray, position: Long, offset: Int, count: Int): Int {
            if (count <= 0) {
                return 0
            }

            val range = "bytes=$position-${position + count - 1}"

            logger.debug { "Request range: $range in file ${s3Object.key()}" }

            return s3.getObject(GetObjectRequest.builder()
                    .range(range)
                    .requestOverrideConfig(requestOverrideConfig)
                    .bucket(bucketName)
                    .key(s3Object.key()).build(),
                    ByteArrayAsyncResponseHandler<GetObjectResponse?>(output, offset)
            ).get()
        }
    }

    internal inner class S3FolderBlock(val _address: Long, private val folderName: String) : Block {

        internal val blocks by lazy {
            val builder = listObjectsBuilder().prefix("_$folderName/")
            s3Objects(builder) { it.key().isValidSubFolder }
                    .asSequence()
                    .map {
                        S3Block(decodeAddress(it.key().split("/")[1]), it)
                    }
                    .sortedBy { it._address }
                    .toList()
        }

        override fun getAddress() = _address

        override fun setReadOnly() = true

        override fun length(): Long = blocks.fold(0L) { acc, value -> acc + value.length() }

        override fun read(output: ByteArray, position: Long, offset: Int, count: Int): Int {
            if (count <= 0) {
                return 0
            }

            val firstIndex = blocks.indexOfFirst { (it.address - address + it.length()) >= position }
            var lastIndex = blocks.indexOfLast { (it.address - address) <= position + count }
            if (firstIndex < 0) {
                return 0
            }
            val first = blocks[firstIndex]
            if (firstIndex == lastIndex) {
                return first.read(output, position - (first.address - address), offset, count)
            }
            if (lastIndex < 0) {
                lastIndex = blocks.size - 1
            }
            val last = blocks[lastIndex]

            var written = 0
            written += first.read(output, position - (first.address - address), offset, first.length().toInt())
            if (firstIndex + 1 < lastIndex) {
                ((firstIndex + 1)..(lastIndex - 1)).forEach {
                    val block = blocks[it]
                    written += block.read(output, 0, offset + (block.address - address - position).toInt(), block.length().toInt())
                }
            }
            written += last.read(output, 0, offset + (last.address - address - position).toInt(), minOf((position + count - last.address).toInt(), last.length().toInt()))
            return written
        }
    }

    private val fileBlocks: List<S3Block>
        get() {
            val builder = listObjectsBuilder().delimiter("/")
            return s3Objects(builder) { it.key().isValidAddress }
                    .asSequence()
                    .map { S3Block(it.key().address, it) }
                    .sortedBy { it._address }
                    .toList()
        }

    private val folderBlocks: List<S3FolderBlock>
        get() {
            val builder = listObjectsBuilder().prefix("_")
            return s3Objects(builder) { it.key().isValidSubFolder }
                    .asSequence()
                    .map {
                        val folderName = it.key().split("/")[0].drop(1)
                        S3FolderBlock(folderName.toFileName().address, folderName)
                    }
                    .sortedBy { it._address }
                    .toList()
        }

    private fun listObjectsBuilder(): ListObjectsRequest.Builder {
        return ListObjectsRequest.builder()
                .requestOverrideConfig(requestOverrideConfig)
                .bucket(bucketName)
    }

    private fun s3Objects(builder: ListObjectsRequest.Builder, filter: (S3Object) -> Boolean): List<S3Object> {
        val result = ArrayList<S3Object>()
        while (true) {
            val response = s3.listObjects(builder.build()).get()
            response.contents()?.let {
                result.addAll(it.filter(filter))
            }
            if (!response.isTruncated) {
                break
            }
            builder.marker(response.marker())
        }
        return result
    }

    private fun String.toFileName() = this + LogUtil.LOG_FILE_EXTENSION
    private val String.address get() = LogUtil.getAddress(this)
    private val String.isValidAddress get() = this.length == LogUtil.LOG_FILE_NAME_WITH_EXT_LENGTH && LogUtil.isLogFileName(this)

    private fun S3Block.truncate(length: Long) {
        logger.debug { "truncating block at ${s3Object.key()} to $length" }
        try {
            val array = ByteArray(length.toInt())
            read(array, 0, 0, length.toInt())
            s3.putObject(PutObjectRequest.builder()
                    .bucket(bucketName)
                    .requestOverrideConfig(requestOverrideConfig)
                    .key(s3Object.key())
                    .build(), object : AsyncRequestProvider {

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
            val msg = "failed to update ${s3Object.key()}"
            logger.error(msg, e)
            throw ExodusException(msg, e)
        }
    }

    private fun List<String>.deleteS3Objects() {
        logger.info { "deleting files ${joinToString()}" }

        s3.deleteObjects(DeleteObjectsRequest.builder()
                .requestOverrideConfig(requestOverrideConfig)
                .delete(
                        Delete.builder().objects(
                                map { ObjectIdentifier.builder().key(it).build() })
                                .build())
                .bucket(bucketName)
                .build()).get()
    }


    private val String.isValidSubFolder: Boolean
        get() {
            val paths = this.split("/")
            val isValidFolderName = paths[0].let {
                it.startsWith("_") && it.drop(1).toFileName().isValidAddress
            }
            return paths.size == 2 && checkAddress(paths[1]) && isValidFolderName
        }

    class NoBlock(private val address: Long) : Block {
        override fun getAddress() = address

        override fun length() = 0L

        override fun read(output: ByteArray, position: Long, offset: Int, count: Int) = 0

        override fun setReadOnly() = false
    }
}
