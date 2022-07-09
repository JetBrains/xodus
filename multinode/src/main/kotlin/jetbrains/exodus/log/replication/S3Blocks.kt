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
import jetbrains.exodus.core.dataStructures.persistent.PersistentBitTreeLongMap
import jetbrains.exodus.core.dataStructures.persistent.PersistentLongMap
import jetbrains.exodus.core.dataStructures.persistent.read
import jetbrains.exodus.core.dataStructures.persistent.write
import jetbrains.exodus.io.Block
import jetbrains.exodus.log.LogUtil
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import java.nio.ByteBuffer
import java.util.*

internal abstract class BasicS3Block(internal val s3factory: S3FactoryBoilerplate,
                                     internal val addr: Long,
                                     internal val size: Long) : Block {

    abstract val key: String

    override fun getAddress() = addr

    override fun length(): Long = size

    override fun read(output: ByteArray, position: Long, offset: Int, count: Int): Int {
        if (count <= 0) {
            return 0
        }

        val range = "bytes=$position-${position + count - 1}"

        S3DataReader.logger.debug { "Request range: $range in file $key" }

        return s3factory.s3.getObject(GetObjectRequest.builder()
                .range(range)
                .overrideConfiguration(s3factory.requestOverrideConfig)
                .bucket(s3factory.bucket)
                .key(key).build(),
                ByteArrayAsyncResponseHandler<GetObjectResponse?>(output, offset)
        ).get()
    }

    override fun read(output: ByteBuffer, position: Long, offset: Int, count: Int): Int {
        if (count <= 0) {
            return 0
        }

        val range = "bytes=$position-${position + count - 1}"

        S3DataReader.logger.debug { "Request range: $range in file $key" }


        val buffer = output.slice(offset, output.limit() - offset)


        return s3factory.s3.getObject(GetObjectRequest.builder()
                .range(range)
                .overrideConfiguration(s3factory.requestOverrideConfig)
                .bucket(s3factory.bucket)
                .key(key).build(),
                ByteBufferAsyncResponseHandler<GetObjectResponse?>(buffer)
        ).get()
    }

    override fun refresh() = this

    internal fun readAndCompare(output: ByteArray, position: Long, offset: Int, count: Int): Int {
        return read(output, position, offset, count).also { read ->
            if (read < count) {
                val msg = "Tried to read $count bytes from $key but got $read bytes"
                S3DataReader.logger.error(msg)
                throw ExodusException(msg)
            }
        }
    }

    internal fun readAndCompare(output: ByteBuffer, position: Long, offset: Int, count: Int): Int {
        return read(output, position, offset, count).also { read ->
            if (read < count) {
                val msg = "Tried to read $count bytes from $key but got $read bytes"
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
        get() = LogUtil.getLogFilename(addr)
}

internal class S3SubBlock(s3factory: S3FactoryBoilerplate,
                          address: Long,
                          size: Long,
                          private val parentAddress: Long) : BasicS3Block(s3factory, address, size) {
    override val key: String
        get() = getPartialFolderPrefix(parentAddress) + getPartialFileName(addr)
}

internal val S3DataReaderOrWriter.fileBlocks: List<S3Block>
    get() {
        val builder = listObjectsBuilder().delimiter("/")
        return listObjects(s3, builder)
                .filter { it.key().isValidAddress }
                .map { S3Block(this, it.key().address, it.size()) }
                .sortedBy { it.addr }
                .toList()
    }

internal val S3DataReaderOrWriter.folderBlocks: List<S3FolderBlock>
    get() {
        val builder = listObjectsBuilder().prefix("_")
        val folders = TreeMap<String, Pair<Long, PersistentLongMap<S3SubBlock>>>()
        listObjects(s3, builder).forEach {
            val paths = it.key().split("/")
            if (paths.size == 2) {
                val folderName = paths[0]
                folders[folderName]?.let { pair ->
                    if (checkAddress(paths[1])) {
                        pair.second.write {
                            val address = decodeAddress(paths[1])
                            put(address, (S3SubBlock(this@folderBlocks, address, it.size(), pair.first)))
                        }
                    }
                } ?: if (folderName.startsWith("_")) {
                    val folderAsFileName = folderName.drop(1).toFileName()
                    if (folderAsFileName.isValidAddress) {
                        if (checkAddress(paths[1])) {
                            val blockAddress = folderAsFileName.address
                            folders[folderName] = blockAddress to
                                    PersistentBitTreeLongMap<S3SubBlock>().apply {
                                        write {
                                            val address = decodeAddress(paths[1])
                                            put(address, S3SubBlock(this@folderBlocks, address, it.size(), blockAddress))
                                        }
                                    }
                        }
                    }
                }
            }
        }
        return folders.values.asSequence().map {
            S3FolderBlock(
                    this,
                    it.first,
                    it.second.read { fold(0L) { acc, value -> acc + value.value.length() } },
                    it.second)
        }.toList()
    }