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
import software.amazon.awssdk.services.s3.model.CopyObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.nio.ByteBuffer
import java.util.*
import kotlin.Comparator
import kotlin.collections.ArrayList

class S3DataReader(
        val s3: S3AsyncClient,
        val bucketName: String,
        val requestOverrideConfig: AwsRequestOverrideConfig? = null,
        val writer: S3DataWriter) : DataReader {

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
        val keysToDelete = ArrayList<String>()

        fileBlocks.filter { it.address == blockAddress }.forEach {
            keysToDelete.add(it.key)
        }

        folderBlocks.filter { it.address == blockAddress }.flatMap { it.blocks }.forEach {
            keysToDelete.add(it.key)
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
            keysToDelete.deleteS3Objects(s3, bucketName, requestOverrideConfig)
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
                it.asSequence()
                        .filter { it.address - folder.address >= length }
                        .map { it.key }
                        .windowed(size = deletePackSize, step = deletePackSize, partialWindows = true).forEach {
                            it.deleteS3Objects(s3, bucketName, requestOverrideConfig)
                        }
            }
        }
    }

    override fun close() = s3.close()

    override fun getBlock(address: Long): Block {
        logger.debug { "Get block at ${LogUtil.getLogFilename(address)}" }
        return blocks.firstOrNull { it.address == address } ?: InMemoryBlock(address)
    }

    internal abstract inner class BasicS3Block(internal val _address: Long, internal val size: Long) : Block {

        abstract val key: String

        override fun getAddress() = _address

        override fun setReadOnly() = true

        override fun length(): Long = size

        override fun read(output: ByteArray, position: Long, offset: Int, count: Int): Int {
            if (count <= 0) {
                return 0
            }

            val range = "bytes=$position-${position + count - 1}"

            logger.debug { "Request range: $range in file $key" }

            return s3.getObject(GetObjectRequest.builder()
                    .range(range)
                    .requestOverrideConfig(requestOverrideConfig)
                    .bucket(bucketName)
                    .key(key).build(),
                    ByteArrayAsyncResponseHandler<GetObjectResponse?>(output, offset)
            ).get()
        }
    }

    internal inner class S3Block(address: Long, size: Long) : BasicS3Block(address, size) {
        override val key: String
            get() = LogUtil.getLogFilename(_address)
    }

    internal inner class S3SubBlock(address: Long, size: Long, private val parentAddress: Long) : BasicS3Block(address, size) {
        override val key: String
            get() = S3DataWriter.getPartialFolderPrefix(parentAddress) + S3DataWriter.getPartialFileName(_address)
    }

    internal inner class S3FolderBlock(private val _address: Long, internal val blocks: List<S3SubBlock>) : Block {
        override fun getAddress() = _address

        override fun setReadOnly() = true

        override fun length(): Long = blocks.fold(0L) { acc, value -> acc + value.length() }

        override fun read(output: ByteArray, position: Long, offset: Int, count: Int): Int {
            if (count <= 0) {
                return 0
            }

            val firstIndex = blocks.indexOfFirst { (it.address - address + it.length()) >= position }
            var lastIndex = blocks.indexOfLast { (it.address - address) <= position + count }

            var totalRead: Int
            if (firstIndex < 0) {
                totalRead = 0
            } else {
                val first = blocks[firstIndex]

                if (firstIndex == lastIndex) {
                    totalRead = first.read(output, position - (first.address - address), offset, count)
                } else {
                    totalRead = 0
                    if (lastIndex < 0) {
                        lastIndex = blocks.size - 1
                    }
                    val last = blocks[lastIndex]

                    val startPosition = position - (first.address - address)
                    totalRead += first.readAndCompare(output, startPosition, offset, (first.length() - startPosition).toInt())
                    if (firstIndex + 1 < lastIndex) {
                        ((firstIndex + 1)..(lastIndex - 1)).forEach {
                            val block = blocks[it]
                            totalRead += block.readAndCompare(output, 0, offset + (block.address - address - position).toInt(), block.length().toInt())
                        }
                    }
                    val outputOffset = (last.address - address - position).toInt()
                    totalRead += last.read(output, 0, offset + outputOffset, minOf(count - (last.address - address - position).toInt(), last.length().toInt()))
                }
            }

            if (totalRead < count) {
                writer.currentFile.get()?.let { memory ->
                    if (memory.blockAddress == _address) {
                        totalRead = memory.read(output, position, totalRead, count, offset)
                    }
                }
            }

            return totalRead
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is S3FolderBlock) return false

            if (_address != other._address) return false

            return true
        }

        override fun hashCode(): Int {
            return _address.hashCode()
        }
    }

    private val fileBlocks: List<S3Block>
        get() {
            val builder = listObjectsBuilder().delimiter("/")
            return listObjects(s3, builder)
                    .filter { it.key().isValidAddress }
                    .map { S3Block(it.key().address, it.size()) }
                    .sortedBy { it._address }
                    .toList()
        }

    private val folderBlocks: List<S3FolderBlock>
        get() {
            val builder = listObjectsBuilder().prefix("_")
            val folders = TreeMap<String, Pair<Long, MutableList<S3SubBlock>>>()
            listObjects(s3, builder).forEach {
                val paths = it.key().split("/")
                if (paths.size == 2) {
                    val folderName = paths[0]
                    folders[folderName]?.let { pair ->
                        if (checkAddress(paths[1])) {
                            pair.second.add(S3SubBlock(decodeAddress(paths[1]), it.size(), pair.first))
                        }
                    } ?: if (folderName.startsWith("_")) {
                        val folderAsFileName = folderName.drop(1).toFileName()
                        if (folderAsFileName.isValidAddress) {
                            if (checkAddress(paths[1])) {
                                val blockAddress = folderAsFileName.address
                                folders[folderName] = blockAddress to mutableListOf(S3SubBlock(decodeAddress(paths[1]), it.size(), blockAddress))
                            }
                        }
                    }
                }
            }
            return folders.values.asSequence().map { S3FolderBlock(it.first, it.second) }.toList()
        }

    private fun listObjectsBuilder() = listObjectsBuilder(bucketName, requestOverrideConfig)

    private fun BasicS3Block.readAndCompare(output: ByteArray, position: Long, offset: Int, count: Int): Int {
        return read(output, position, offset, count).also {
            if (it < count) {
                val msg = "try to read $count bytes from $key but get $it"
                logger.error(msg)
                throw ExodusException(msg)
            }
        }
    }

    private fun BasicS3Block.truncate(length: Long) {
        logger.debug { "truncating block at $key to $length" }
        try {
            val array = ByteArray(length.toInt())
            read(array, 0, 0, length.toInt())
            s3.putObject(PutObjectRequest.builder()
                    .bucket(bucketName)
                    .requestOverrideConfig(requestOverrideConfig)
                    .key(key)
                    .contentLength(length)
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
            val msg = "failed to update $key"
            logger.error(msg, e)
            throw ExodusException(msg, e)
        }
    }

    internal inner class InMemoryBlock(private val address: Long) : Block {
        override fun getAddress() = address

        override fun length() = 0L

        override fun read(output: ByteArray, position: Long, offset: Int, count: Int): Int {
            if (count > 0) {
                writer.currentFile.get()?.let { memory ->
                    if (memory.blockAddress == address) {
                        return memory.read(output, position, 0, count, offset)
                    }
                }
            }
            return 0
        }

        override fun setReadOnly() = false
    }
}
