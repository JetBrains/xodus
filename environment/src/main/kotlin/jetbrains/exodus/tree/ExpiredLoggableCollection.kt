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
import it.unimi.dsi.fastutil.ints.IntBinaryOperator
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
import it.unimi.dsi.fastutil.longs.LongArrayList
import jetbrains.exodus.ExodusException
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.Loggable
import java.lang.UnsupportedOperationException

const val nonAccumulatedStatsLimit = 1_000

interface ExpiredLoggableCollection {
    val size: Int
    fun add(loggable: Loggable)

    fun add(address: Long, length: Int)

    fun trimToSize(): ExpiredLoggableCollection

    fun mergeWith(parent: ExpiredLoggableCollection): ExpiredLoggableCollection

    fun forEach(action: (Long, Int) -> Unit)

    companion object {
        @JvmStatic
        val EMPTY = EmptyLoggableCollection()

        @JvmStatic
        fun newInstance(log: Log) = MutableExpiredLoggableCollection(log)
    }
}

class MutableExpiredLoggableCollection(
    private val log: Log,
    private var parent: MutableExpiredLoggableCollection? = null,
    private val addresses: LongArrayList = LongArrayList(),
    private val lengths: IntArrayList = IntArrayList(),
    private var accumulatedStats: Long2IntOpenHashMap? = null
) : ExpiredLoggableCollection {

    private var _size: Int = 0

    override val size : Int  get()  = _size + (parent?.size ?: 0)

    private val nonAccumulatedSize: Int
        get() = lengths.size + (parent?.run { lengths.size + (accumulatedStats?.size ?: 0) } ?: 0)

    override fun add(loggable: Loggable) = add(loggable.address, loggable.length())

    override fun add(address: Long, length: Int) {
        addresses.add(address)
        lengths.add(length)

        _size++

        accumulateStats()
    }

    private fun accumulateStats() {
        if (nonAccumulatedSize >= nonAccumulatedStatsLimit) {
            if (accumulatedStats == null) {
                accumulatedStats = Long2IntOpenHashMap()
            }

            var currentParent = parent
            var currentLengths: IntArrayList? = lengths
            var currentAddresses: LongArrayList? = addresses
            var currentAccumulatedStats: Long2IntOpenHashMap? = null

            while (currentAddresses != null && currentLengths != null) {
                for (i in 0 until currentLengths.size) {
                    val currentAddress = currentAddresses.getLong(i)
                    val currentFileAddress = log.getFileAddress(currentAddress)
                    val currentLength = currentLengths.getInt(i)

                    accumulatedStats!!.mergeInt(currentFileAddress, currentLength, (IntBinaryOperator { space, diff ->
                        return@IntBinaryOperator space + diff
                    }))
                }

                currentAccumulatedStats?.long2IntEntrySet()?.fastForEach {
                    accumulatedStats!!.mergeInt(it.longKey, it.intValue, (IntBinaryOperator { space, diff ->
                        return@IntBinaryOperator space + diff
                    }))
                }


                currentParent?.apply {
                    currentLengths = lengths
                    currentAddresses = addresses
                    currentParent = parent
                    currentAccumulatedStats = accumulatedStats
                } ?: break
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
            return (this.parent?.let {
                parent.parent?.let {
                    throw ExodusException("Can't merge 2 ExpiredLoggableCollections with both non-trivial parents")
                }
                MutableExpiredLoggableCollection(log, this, parent.addresses, parent.lengths, parent.accumulatedStats)
            } ?: MutableExpiredLoggableCollection(log, parent, addresses, lengths, accumulatedStats)).apply {
                accumulateStats()
            }
        }

        return this
    }

    override fun forEach(action: (Long, Int) -> Unit) {
        var current: MutableExpiredLoggableCollection? = this

        while (current != null) {
            current.apply {
                for (i in 0 until lengths.size) {
                    action(addresses.getLong(i), lengths.getInt(i))
                }

                accumulatedStats?.long2IntEntrySet()?.fastForEach { entry ->
                    action(entry.longKey, entry.intValue)
                }
            }
            current = current.parent
        }
    }

    override fun toString() = "Expired $size loggables"
}

class EmptyLoggableCollection : ExpiredLoggableCollection {
    override val size: Int
        get() = 0

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
        if (parent.size > 0) {
            throw UnsupportedOperationException()
        }

        return this
    }

    override fun forEach(action: (Long, Int) -> Unit) {
    }
}