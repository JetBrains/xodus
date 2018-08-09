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

import jetbrains.exodus.core.dataStructures.persistent.PersistentBitTreeLongMap
import jetbrains.exodus.core.dataStructures.persistent.PersistentLongMap
import jetbrains.exodus.core.dataStructures.persistent.read
import jetbrains.exodus.core.dataStructures.persistent.write

internal class S3FolderBlock(s3factory: S3FactoryBoilerplate,
                             address: Long,
                             internal val blocks: PersistentLongMap<S3SubBlock>,
                             private val currentFile: S3DataWriter.CurrentFile?) : BasicS3Block(s3factory, address, 0) {
    override val key: String
        get() = getPartialFolderPrefix(_address)

    override fun length(): Long = blocks.read { fold(0L) { acc, value -> acc + value.value.length() } }

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
            val bytesToRead = Math.min((block.length() - blockPosition).toInt(), count - totalRead)
            block.readAndCompare(output, blockPosition, offset + totalRead, bytesToRead).also {
                totalRead += it
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

    override fun refresh(): S3FolderBlock {
        return newS3FolderBlock(s3factory, _address)
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

internal fun newS3FolderBlock(s3factory: S3FactoryBoilerplate, address: Long): S3FolderBlock {
    val builder = s3factory.listObjectsBuilder().prefix(getPartialFolderPrefix(address))
    val subBlocks = PersistentBitTreeLongMap<S3SubBlock>()
    subBlocks.write {
        listObjects(s3factory.s3, builder).forEach {
            val paths = it.key().split("/")
            if (paths.size == 2) {
                val subBlockAddress = decodeAddress(paths[1])
                put(subBlockAddress, S3SubBlock(s3factory, subBlockAddress, it.size(), address))
            }
        }
    }
    return S3FolderBlock(s3factory, address, subBlocks, null)
}