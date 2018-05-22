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

import jetbrains.exodus.io.Block
import jetbrains.exodus.io.DataReader
import jetbrains.exodus.io.RemoveBlockType
import jetbrains.exodus.log.LogUtil
import mu.KLogging
import software.amazon.awssdk.core.AwsRequestOverrideConfig
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.*
import java.util.*
import kotlin.Comparator


class S3DataReader(
        val s3: S3AsyncClient,
        val bucketName: String,
        val requestOverrideConfig: AwsRequestOverrideConfig? = null) : DataReader {

    companion object : KLogging()

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
        // TODO
    }

    override fun truncateBlock(blockAddress: Long, length: Long) {
        TODO("not implemented")
    }

    override fun close() = s3.close()

    override fun getBlock(address: Long): Block {
        logger.info { "Get block at ${LogUtil.getLogFilename(address)}" }
        return blocks.first { it.address == address }
    }

    override fun clear() {
        val builder = listObjectsBuilder()
        while (true) {
            val response = s3.listObjects(builder.build()).get()
            response.contents()?.let {
                s3.deleteObjects(DeleteObjectsRequest.builder()
                        .requestOverrideConfig(requestOverrideConfig)
                        .delete(
                                Delete.builder().objects(
                                        it
                                                .filter { it.key().isValidAddress || it.key().isValidSubFolder }
                                                .map { ObjectIdentifier.builder().key(it.key()).build() })
                                        .build())
                        .bucket(bucketName)
                        .build()).get()
            }
            if (!response.isTruncated) {
                break
            }
            builder.marker(response.marker())
        }
    }

    internal inner class S3Block(val _address: Long, private val s3Object: S3Object) : Block {

        override fun getAddress() = _address

        override fun setReadOnly() = true

        override fun length(): Long = s3Object.size()

        override fun read(output: ByteArray, position: Long, offset: Int, count: Int): Int {
            if (count <= 0) {
                return 0
            }
            val range = "bytes=$position-${position + count - 1}"

            logger.debug { "Request range: $range" }
            val written = s3.getObject(GetObjectRequest.builder()
                    .range(range)
                    .requestOverrideConfig(requestOverrideConfig)
                    .bucket(bucketName)
                    .key(s3Object.key()).build(),
                    ByteArrayAsyncResponseHandler(output, offset)
            ).get()

            if (written < count) {
                logger.debug { "Read underflow: expected $count, got $written" }
            }

            return written
        }
    }

    internal inner class S3FolderBlock(val _address: Long, private val folderName: String) : Block {

        private val blocks by lazy {
            val builder = listObjectsBuilder().prefix("_$folderName/")
            s3Objects(builder) { it.key().isValidSubFolder }
                    .asSequence()
                    .map {
                        S3Block(it.key().split("/")[1].address, it)
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
            val firstIndex = blocks.indexOfFirst { it.address + it.length() >= position }
            val lastIndex = blocks.indexOfFirst { it.address + it.length() <= position + count }

            val first = blocks[firstIndex]
            val last = blocks[lastIndex]
            var written = 0
            written += first.read(output, first.address, offset, first.length().toInt())
            written += last.read(output, last.address, offset + count, last.length().toInt())
            if (firstIndex + 1 > lastIndex) {
                ((firstIndex + 1)..lastIndex).forEach {
                    val block = blocks[it]
                    written += block.read(output, block.address, offset + (block.address - position).toInt(), block.length().toInt())
                }
            }
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
        val result = LinkedList<S3Object>()
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

    private val String.isValidSubFolder: Boolean
        get() {
            val paths = this.split("/")
            val isValidFolderName = paths[0].let {
                it.startsWith("_") && it.drop(1).toFileName().isValidAddress
            }
            return paths.size == 2 && paths[1].isValidAddress && isValidFolderName
        }

}