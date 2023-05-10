/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.log

import jetbrains.exodus.core.dataStructures.hash.LongIterator
import jetbrains.exodus.core.dataStructures.persistent.PersistentBitTreeLongMap
import jetbrains.exodus.core.dataStructures.persistent.PersistentLongMap
import jetbrains.exodus.io.Block

// block key is aligned block address, i.e. block address divided by blockSize
sealed class BlockSet(@JvmField val blockSize: Long, @JvmField val set: PersistentLongMap<Block>) {
    protected abstract fun getCurrent(): PersistentLongMap.ImmutableMap<Block>

    fun size() = getCurrent().size()

    fun isEmpty() = getCurrent().isEmpty

    fun getMinimum(): Long? {
        val iterator = getCurrent().iterator()

        if (iterator.hasNext()) {
            return iterator.next().key.keyToAddress()
        }

        return null
    }

    fun getMaximum(): Long? {
        val iterator = getCurrent().reverseIterator()
        if (iterator.hasNext()) {
            return iterator.next().key.keyToAddress()
        }

        return null
    }

    fun getFiles(reversed: Boolean = false): LongArray {
        val current = getCurrent()
        val result = LongArray(current.size())
        val it = if (reversed) {
            current.reverseIterator()
        } else {
            current.iterator()
        }
        for (i in 0 until result.size) {
            result[i] = it.next().key.keyToAddress()
        }
        return result
    }

    fun contains(blockAddress: Long) = getCurrent().containsKey(blockAddress.addressToKey())

    fun getBlock(blockAddress: Long): Block =
        getCurrent().get(blockAddress.addressToKey()) ?: EmptyBlock(blockAddress)

    // if address is inside of a block, the block containing it must be included as well if present
    fun getFilesFrom(blockAddress: Long = 0L): LongIterator = object : LongIterator {
        val current = getCurrent()
        val it =
            if (blockAddress == 0L) current.iterator() else current.tailEntryIterator(blockAddress.addressToKey())

        override fun next() = nextLong()

        override fun hasNext() = it.hasNext()

        override fun nextLong() = it.next().key.keyToAddress()

        override fun remove() = throw UnsupportedOperationException()
    }

    fun beginWrite() = Mutable(blockSize, set.clone)


    protected fun Long.keyToAddress(): Long = this * blockSize

    protected fun Long.addressToKey(): Long = this / blockSize

    class Immutable(
        blockSize: Long,
        map: PersistentLongMap<Block> = PersistentBitTreeLongMap()
    ) : BlockSet(blockSize, map) {
        private val immutable: PersistentLongMap.ImmutableMap<Block> = map.beginRead()

        public override fun getCurrent(): PersistentLongMap.ImmutableMap<Block> = immutable
    }

    class Mutable(blockSize: Long, map: PersistentLongMap<Block>) : BlockSet(blockSize, map) {
        private val mutable: PersistentLongMap.MutableMap<Block> = set.beginWrite()

        override fun getCurrent(): PersistentLongMap.ImmutableMap<Block> = mutable

        fun clear() = mutable.clear()

        fun add(blockAddress: Long, block: Block) = mutable.put(blockAddress.addressToKey(), block)

        fun remove(blockAddress: Long) = mutable.remove(blockAddress.addressToKey()) != null

        fun endWrite(): Immutable {
            if (!mutable.endWrite()) {
                throw IllegalStateException("File set can't be updated")
            }
            return Immutable(blockSize, set.clone)
        }
    }

    private inner class EmptyBlock(private val address: Long) : Block {
        override fun getAddress() = address

        override fun length() = 0L

        override fun read(output: ByteArray, position: Long, offset: Int, count: Int) = 0

        override fun refresh() = this
    }
}
