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
package jetbrains.exodus.tree

import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
import it.unimi.dsi.fastutil.longs.LongArrayList
import jetbrains.exodus.log.*
import java.util.function.BiConsumer


private const val NON_ACCUMULATED_STATS_LIMIT = 1000

interface ExpiredLoggableCollection {
    fun size(): Int
    fun add(loggable: Loggable)
    fun add(address: Long, length: Int)
    fun trimToSize(): ExpiredLoggableCollection
    fun mergeWith(parent: ExpiredLoggableCollection): ExpiredLoggableCollection
    fun forEach(action: BiConsumer<Long, Int>)

    companion object {
        fun newInstance(log: Log): ExpiredLoggableCollection {
            return MutableExpiredLoggableCollection(log)
        }

        @JvmField
        val EMPTY: ExpiredLoggableCollection = EmptyLoggableCollection()
    }
}

internal class MutableExpiredLoggableCollection(
    private val log: Log,
    private var parent: MutableExpiredLoggableCollection? = null,
    private val addresses: LongArrayList = LongArrayList(),
    private val lengths: IntArrayList = IntArrayList(),
    private var accumulatedStats: Long2IntOpenHashMap? = null
) : ExpiredLoggableCollection {
    private var size = 0
    override fun size(): Int {
        var sum = size
        val parent = parent

        if (parent != null) {
            sum += parent.size()
        }

        return sum
    }

    private fun getNonAccumulatedSize(): Int {
        var sum = lengths.size
        val parent = parent

        if (parent != null) {
            sum += parent.lengths.size

            val accumulatedStats = parent.accumulatedStats
            if (accumulatedStats != null) {
                sum += accumulatedStats.size
            }
        }

        return sum
    }

    override fun add(loggable: Loggable) {
        add(loggable.getAddress(), loggable.length())
    }

    override fun add(address: Long, length: Int) {
        addresses.add(address)
        lengths.add(length)
        size++
        accumulateStats()
    }

    private fun accumulateStats() {
        if (getNonAccumulatedSize() >= NON_ACCUMULATED_STATS_LIMIT) {
            if (accumulatedStats == null) {
                accumulatedStats = Long2IntOpenHashMap()
            }
            var currentParent = parent
            var currentLengths = lengths
            var currentAddresses = addresses
            var currentAccumulatedStats: Long2IntOpenHashMap? = null
            while (true) {
                for (i in currentLengths.indices) {
                    val currentAddress = currentAddresses.getLong(i)
                    val currentFileAddress = log.getFileAddress(currentAddress)
                    val currentLength = currentLengths.getInt(i)
                    accumulatedStats!!.mergeInt(
                        currentFileAddress,
                        currentLength
                    ) { a: Int, b: Int -> Integer.sum(a, b) }
                }
                if (currentAccumulatedStats != null) {
                    for (entry in currentAccumulatedStats.long2IntEntrySet()) {
                        accumulatedStats!!.mergeInt(
                            entry.longKey,
                            entry.intValue
                        ) { a: Int, b: Int -> Integer.sum(a, b) }
                    }
                }
                if (currentParent != null) {
                    currentLengths = currentParent.lengths
                    currentAddresses = currentParent.addresses
                    currentParent = currentParent.parent
                    currentAccumulatedStats = currentParent?.accumulatedStats
                } else {
                    break
                }
            }
            parent = null
            addresses.clear()
            lengths.clear()
        }
    }

    override fun trimToSize(): ExpiredLoggableCollection {
        addresses.trim()
        lengths.trim()
        return this
    }

    override fun mergeWith(parent: ExpiredLoggableCollection): ExpiredLoggableCollection {
        if (parent is MutableExpiredLoggableCollection) {
            val parentAsMutable = parent
            return (if (this.parent != null) MutableExpiredLoggableCollection(
                log,
                this, parentAsMutable.addresses, parentAsMutable.lengths, parentAsMutable.accumulatedStats
            ) else MutableExpiredLoggableCollection(log, parentAsMutable, addresses, lengths, accumulatedStats))
                .applyAccumulateStats()
        }
        return this
    }

    private fun applyAccumulateStats(): MutableExpiredLoggableCollection {
        accumulateStats()
        return this
    }

    override fun forEach(action: BiConsumer<Long, Int>) {
        var current: MutableExpiredLoggableCollection? = this
        while (current != null) {
            for (i in current.lengths.indices) {
                action.accept(current.addresses.getLong(i), current.lengths.getInt(i))
            }
            if (current.accumulatedStats != null) {
                for (entry in current.accumulatedStats!!.long2IntEntrySet()) {
                    action.accept(entry.longKey, entry.intValue)
                }
            }
            current = current.parent
        }
    }

    override fun toString(): String {
        return "Expired ${size()} loggables"
    }
}

internal class EmptyLoggableCollection : ExpiredLoggableCollection {
    override fun size(): Int = 0

    override fun add(loggable: Loggable) {
        throw UnsupportedOperationException()
    }

    override fun add(address: Long, length: Int) {
        throw UnsupportedOperationException()
    }

    override fun trimToSize(): ExpiredLoggableCollection {
        return this
    }

    override fun mergeWith(parent: ExpiredLoggableCollection): ExpiredLoggableCollection {
        if (parent.size() > 0) {
            throw UnsupportedOperationException()
        }
        return this
    }

    override fun forEach(action: BiConsumer<Long, Int>) {}
}
