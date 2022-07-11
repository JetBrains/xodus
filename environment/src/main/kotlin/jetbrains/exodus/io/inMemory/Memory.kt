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
package jetbrains.exodus.io.inMemory

import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.dataStructures.LongObjectCache
import jetbrains.exodus.core.dataStructures.hash.LongHashMap
import jetbrains.exodus.kotlin.synchronized
import jetbrains.exodus.log.LogUtil
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer

open class Memory {
    private var lastBlock: Block? = null
    private val data = LongHashMap<Block>()
    private val removedBlocks = LongObjectCache<Block>(100)

    internal val allBlocks: Collection<Block>
        get() = data.values

    internal fun getOrCreateBlockData(address: Long, length: Long): Block {
        return data.synchronized {
            get(address)?.also {
                if (it.size.toLong() != length) {
                    it.setSize(length)
                }
                lastBlock = it
            } ?: run {
                val block = Block(address, lastBlock?.size ?: 2048)
                lastBlock = block
                this[address] = lastBlock
                block
            }
        }
    }

    internal fun removeBlock(blockAddress: Long): Boolean {
        val removed = data.synchronized {
            remove(blockAddress)
        }
        removed?.let {
            data.synchronized {
                removedBlocks.cacheObject(blockAddress, removed)
            }
            return true
        }
        return false
    }

    internal fun clear() = data.synchronized { clear() }

    fun dump(location: File) {
        location.mkdirs()
        val saver = { key: Long, block: Block ->
            try {
                val dest = File(location, LogUtil.getLogFilename(key))
                val output = RandomAccessFile(dest, "rw")
                output.write(block.data, 0, block.size)
                output.close()
                // output.getChannel().force(false);
            } catch (e: IOException) {
                throw ExodusException(e)
            }
        }
        data.synchronized {
            forEach(saver)
            removedBlocks.forEachEntry { entry ->
                saver(entry.key, entry.value)
                true
            }
        }
    }

    internal class Block(private val _address: Long, initialSize: Int) : jetbrains.exodus.io.Block {
        var size: Int = 0
            private set
        var data: ByteArray
            private set

        init {
            data = ByteArray(initialSize)
        }

        override fun getAddress() = _address

        override fun length() = size.toLong()

        fun setSize(size: Long) {
            this.size = size.toInt()
        }

        fun write(b: ByteArray, off: Int, len: Int) {
            val newSize = size + len
            ensureCapacity(newSize)
            System.arraycopy(b, off, data, size, len)
            size = newSize
        }

        fun write(b: ByteBuffer, off: Int, len: Int) {
            val newSize = size + len
            ensureCapacity(newSize)
            ByteBuffer.wrap(data).put(size, b, off, len)
            size = newSize
        }

        override fun read(output: ByteArray, position: Long, offset: Int, count: Int): Int {
            var result = count
            if (position < 0) {
                throw ExodusException("Block index out of range, underflow")
            }
            val maxRead = size - position
            if (maxRead < 0) {
                throw ExodusException("Block index out of range")
            }
            if (maxRead < result) {
                result = maxRead.toInt()
            }
            System.arraycopy(data, position.toInt(), output, offset, result)
            return result
        }


        override fun read(output: ByteBuffer, position: Long, offset: Int, count: Int): Int {
            var result = count
            if (position < 0) {
                throw ExodusException("Block index out of range, underflow")
            }
            val maxRead = size - position
            if (maxRead < 0) {
                throw ExodusException("Block index out of range")
            }
            if (maxRead < result) {
                result = maxRead.toInt()
            }

            output.put(offset, ByteBuffer.wrap(data), position.toInt(), result)

            return result
        }


        override fun refresh() = this

        fun ensureCapacity(minCapacity: Int) {
            val oldCapacity = data.size
            if (minCapacity > oldCapacity) {
                val oldData = data
                var newCapacity = oldCapacity * 3 / 2 + 1
                if (newCapacity < minCapacity) newCapacity = minCapacity
                data = ByteArray(newCapacity)
                System.arraycopy(oldData, 0, data, 0, oldCapacity)
            }
        }
    }
}
