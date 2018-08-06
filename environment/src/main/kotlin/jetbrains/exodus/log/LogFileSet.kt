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
package jetbrains.exodus.log

import jetbrains.exodus.core.dataStructures.hash.LongIterator
import jetbrains.exodus.core.dataStructures.persistent.PersistentBitTreeLongMap
import jetbrains.exodus.core.dataStructures.persistent.PersistentLongMap
import jetbrains.exodus.io.Block

// file key is aligned file address, i.e. file address divided by fileSize
sealed class LogFileSet(val fileSize: Long, val set: PersistentLongMap<Block>) {
    protected abstract val current: PersistentLongMap.ImmutableMap<Block>

    fun size() = current.size()

    val isEmpty get() = current.isEmpty

    val minimum: Long? get() = current.iterator().let { if (it.hasNext()) it.next().key.keyToAddress else null }

    val maximum: Long? get() = current.reverseIterator().let { if (it.hasNext()) it.next().key.keyToAddress else null }

    /**
     * Array of files' addresses in reverse order: the newer files first
     */
    val array: LongArray
        get() = getFiles(reversed = true)

    @JvmOverloads
    fun getFiles(reversed: Boolean = false): LongArray {
        val current = current
        val result = LongArray(current.size())
        val it = if (reversed) {
            current.reverseIterator()
        } else {
            current.iterator()
        }
        for (i in 0 until result.size) {
            result[i] = it.next().key.keyToAddress
        }
        return result
    }

    fun contains(fileAddress: Long) = current.containsKey(fileAddress.addressToKey)

    fun getBlock(fileAddress: Long): Block = current.get(fileAddress.addressToKey) ?: EmptyBlock(fileAddress)

    // if address is inside of a file, the file containing it must be included as well if present
    fun getFilesFrom(fileAddress: Long = 0L): LongIterator = object : LongIterator {
        val it = if (fileAddress == 0L) current.iterator() else current.tailEntryIterator(fileAddress.addressToKey)

        override fun next() = nextLong()

        override fun hasNext() = it.hasNext()

        override fun nextLong() = it.next().key.keyToAddress

        override fun remove() = throw UnsupportedOperationException()
    }

    fun beginWrite() = Mutable(fileSize, set.clone)

    protected val Long.keyToAddress: Long get() = this * fileSize

    protected val Long.addressToKey: Long get() = this / fileSize

    class Immutable @JvmOverloads constructor(
            fileSize: Long,
            map: PersistentLongMap<Block> = PersistentBitTreeLongMap()
    ) : LogFileSet(fileSize, map) {
        private val immutable: PersistentLongMap.ImmutableMap<Block> = map.beginRead()

        public override val current: PersistentLongMap.ImmutableMap<Block>
            get() = immutable
    }

    class Mutable(fileSize: Long, map: PersistentLongMap<Block>) : LogFileSet(fileSize, map) {
        private val mutable: PersistentLongMap.MutableMap<Block> = set.beginWrite()

        override val current: PersistentLongMap.ImmutableMap<Block>
            get() = mutable

        fun clear() = mutable.clear()

        fun add(fileAddress: Long, block: Block) = mutable.put(fileAddress.addressToKey, block)

        fun remove(fileAddress: Long) = mutable.remove(fileAddress.addressToKey) != null

        fun endWrite(): Immutable {
            if (!mutable.endWrite()) {
                throw IllegalStateException("File set can't be updated")
            }
            return Immutable(fileSize, set.clone)
        }
    }

    private inner class EmptyBlock(private val address: Long) : Block {
        override fun getAddress() = address

        override fun length() = 0L

        override fun read(output: ByteArray, position: Long, offset: Int, count: Int) = 0
    }
}
