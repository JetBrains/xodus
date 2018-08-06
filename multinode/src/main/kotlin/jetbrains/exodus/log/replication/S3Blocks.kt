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
import jetbrains.exodus.log.LogUtil
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import java.util.*

internal abstract class BasicS3Block(private val s3factory: S3FactoryBoilerplate,
                                     internal val _address: Long,
                                     internal val size: Long) : Block {

    abstract val key: String

    override fun getAddress() = _address

    override fun length(): Long = size

    override fun read(output: ByteArray, position: Long, offset: Int, count: Int): Int {
        if (count <= 0) {
            return 0
        }

        val range = "bytes=$position-${position + count - 1}"

        S3DataReader.logger.debug { "Request range: $range in file $key" }

        return s3factory.s3.getObject(GetObjectRequest.builder()
                .range(range)
                .requestOverrideConfig(s3factory.requestOverrideConfig)
                .bucket(s3factory.bucket)
                .key(key).build(),
                ByteArrayAsyncResponseHandler<GetObjectResponse?>(output, offset)
        ).get()
    }

    internal fun readAndCompare(output: ByteArray, position: Long, offset: Int, count: Int): Int {
        return read(output, position, offset, count).also {
            if (it < count) {
                val msg = "try to read $count bytes from $key but get $it"
                S3DataReader.logger.error(msg)
                throw ExodusException(msg)
            }
        }
    }
}

internal class S3Block(s3factory: S3FactoryBoilerplate,
                       address: Long,
                       size: Long) : BasicS3Block(s3factory, address, size) {
    override val key: String
        get() = LogUtil.getLogFilename(_address)
}

internal class S3SubBlock(s3factory: S3FactoryBoilerplate,
                          address: Long,
                          size: Long,
                          private val parentAddress: Long) : BasicS3Block(s3factory, address, size) {
    override val key: String
        get() = getPartialFolderPrefix(parentAddress) + getPartialFileName(_address)
}

internal class S3FolderBlock(private val _address: Long,
                             internal val blocks: List<S3SubBlock>,
                             private val currentFile: S3DataWriter.CurrentFile?) : Block {

    override fun getAddress() = _address

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
            currentFile?.let { memory ->
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

internal val S3DataReaderOrWriter.fileBlocks: List<S3Block>
    get() {
        val builder = listObjectsBuilder().delimiter("/")
        return listObjects(s3, builder)
                .filter { it.key().isValidAddress }
                .map { S3Block(this, it.key().address, it.size()) }
                .sortedBy { it._address }
                .toList()
    }

internal val S3DataReaderOrWriter.folderBlocks: List<S3FolderBlock>
    get() {
        val builder = listObjectsBuilder().prefix("_")
        val folders = TreeMap<String, Pair<Long, MutableList<S3SubBlock>>>()
        listObjects(s3, builder).forEach {
            val paths = it.key().split("/")
            if (paths.size == 2) {
                val folderName = paths[0]
                folders[folderName]?.let { pair ->
                    if (checkAddress(paths[1])) {
                        pair.second.add(S3SubBlock(this, decodeAddress(paths[1]), it.size(), pair.first))
                    }
                } ?: if (folderName.startsWith("_")) {
                    val folderAsFileName = folderName.drop(1).toFileName()
                    if (folderAsFileName.isValidAddress) {
                        if (checkAddress(paths[1])) {
                            val blockAddress = folderAsFileName.address
                            folders[folderName] = blockAddress to mutableListOf(S3SubBlock(this, decodeAddress(paths[1]), it.size(), blockAddress))
                        }
                    }
                }
            }
        }
        return folders.values.asSequence().map { S3FolderBlock(it.first, it.second, currentFile.get()) }.toList()
    }