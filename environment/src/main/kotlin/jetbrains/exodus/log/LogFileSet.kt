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
import jetbrains.exodus.core.dataStructures.persistent.PersistentBitTreeLongSet
import jetbrains.exodus.core.dataStructures.persistent.PersistentLongSet

private const val BITS_PER_ENTRY = 7

internal open class LogFileSetImmutable @JvmOverloads constructor(
        val fileSize: Long,
        val set: PersistentLongSet = PersistentBitTreeLongSet(BITS_PER_ENTRY),
        // file key is aligned file address, i.e. file address divided by fileSize
        val current: PersistentLongSet.ImmutableSet = set.beginRead()
) {
    fun size() = current.size()

    val isEmpty get() = current.isEmpty

    val minimum: Long? get() = current.longIterator().let { if (it.hasNext()) it.nextLong().keyToAddress else null }

    val maximum: Long? get() = current.reverseLongIterator().let { if (it.hasNext()) it.nextLong().keyToAddress else null }

    /**
     * Array of files' addresses in reverse order: the newer files first
     */
    val array: LongArray
        get() {
            val current = current
            val result = LongArray(current.size())
            val it = current.reverseLongIterator()
            for (i in 0 until result.size) {
                result[i] = it.nextLong().keyToAddress
            }
            return result
        }

    fun contains(fileAddress: Long) = current.contains(fileAddress.addressToKey)

    fun getFilesFrom(fileAddress: Long = 0L): LongIterator = getFilesFrom(current, fileAddress)

    fun getFilesFrom(snapshot: PersistentLongSet.ImmutableSet, fileAddress: Long = 0L): LongIterator {
        return object : LongIterator {
            val it = if (fileAddress == 0L) snapshot.longIterator() else snapshot.tailLongIterator(fileAddress.addressToKey)

            override fun next() = nextLong()

            override fun hasNext() = it.hasNext()

            override fun nextLong() = it.nextLong().keyToAddress

            override fun remove() = throw UnsupportedOperationException()
        }
    }

    fun beginWrite(): LogFileSetMutable {
        val set = this.set.clone
        return LogFileSetMutable(fileSize, set, set.beginWrite())
    }

    protected val Long.keyToAddress: Long get() = this * fileSize

    protected val Long.addressToKey: Long get() = this / fileSize
}

internal class LogFileSetMutable @JvmOverloads constructor(
        fileSize: Long,
        set: PersistentLongSet = PersistentBitTreeLongSet(BITS_PER_ENTRY),
        private val mutable: PersistentLongSet.MutableSet = set.beginWrite()
) : LogFileSetImmutable(fileSize, set, mutable) {


    fun clear() = mutable.clear()

    fun add(fileAddress: Long) = mutable.add(fileAddress.addressToKey)

    fun remove(fileAddress: Long) = mutable.remove(fileAddress.addressToKey)

    fun endWrite(): LogFileSetImmutable {
        mutable.endWrite()
        return LogFileSetImmutable(fileSize, set.clone)
    }
}
