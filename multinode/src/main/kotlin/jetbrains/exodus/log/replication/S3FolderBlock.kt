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

import jetbrains.exodus.core.dataStructures.persistent.PersistentBitTreeLongMap
import jetbrains.exodus.core.dataStructures.persistent.PersistentLongMap
import jetbrains.exodus.core.dataStructures.persistent.read
import jetbrains.exodus.core.dataStructures.persistent.write
import java.nio.ByteBuffer
import kotlin.math.min

internal class S3FolderBlock(s3factory: S3FactoryBoilerplate,
                             address: Long,
                             size: Long,
                             internal val blocks: PersistentLongMap<S3SubBlock>)
    : BasicS3Block(s3factory, address, size) {

    override val key: String get() = getPartialFolderPrefix(addr)

    //override fun length(): Long = blocks.read { fold(0L) { acc, value -> acc + value.value.length() } }

    override fun read(output: ByteArray, position: Long, offset: Int, count: Int): Int {
        if (count <= 0) {
            return 0
        }
        val blocks = mutableListOf<S3SubBlock>()
        val leftBound = address + position
        val rightBound = leftBound + count
        this.blocks.read {
            forEach {
                with(it.value) {
                    if (address >= leftBound) {
                        if (address >= rightBound) return@forEach
                    }
                    blocks.add(this)
                }
            }
        }
        var totalRead = 0
        blocks.forEach { block ->
            val blockPosition = leftBound + totalRead - block.address
            val bytesToRead = min((block.length() - blockPosition).toInt(), count - totalRead)
            block.readAndCompare(output, blockPosition, offset + totalRead, bytesToRead).also {
                totalRead += it
            }
        }
        return totalRead
    }

    override fun read(output: ByteBuffer, position: Long, offset: Int, count: Int): Int {
        if (count <= 0) {
            return 0
        }
        val blocks = mutableListOf<S3SubBlock>()
        val leftBound = address + position
        val rightBound = leftBound + count
        this.blocks.read {
            forEach {
                with(it.value) {
                    if (address >= leftBound) {
                        if (address >= rightBound) return@forEach
                    }
                    blocks.add(this)
                }
            }
        }
        var totalRead = 0
        blocks.forEach { block ->
            val blockPosition = leftBound + totalRead - block.address
            val bytesToRead = min((block.length() - blockPosition).toInt(), count - totalRead)
            block.readAndCompare(output, blockPosition, offset + totalRead, bytesToRead).also {
                totalRead += it
            }
        }
        return totalRead
    }

    override fun refresh(): S3FolderBlock {
        return newS3FolderBlock(s3factory, addr)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is S3FolderBlock) return false

        if (addr != other.addr) return false

        return true
    }

    override fun hashCode(): Int {
        return addr.hashCode()
    }
}

internal fun newS3FolderBlock(s3factory: S3FactoryBoilerplate, address: Long): S3FolderBlock {
    val builder = s3factory.listObjectsBuilder().prefix(getPartialFolderPrefix(address))
    val subBlocks = PersistentBitTreeLongMap<S3SubBlock>()
    var size = 0L
    subBlocks.write {
        listObjects(s3factory.s3, builder).forEach {
            val paths = it.key().split("/")
            if (paths.size == 2) {
                val subBlockAddress = decodeAddress(paths[1])
                val subBlockSize = it.size()
                put(subBlockAddress, S3SubBlock(s3factory, subBlockAddress, subBlockSize, address))
                size += subBlockSize
            }
        }
    }
    return S3FolderBlock(s3factory, address, size, subBlocks)
}