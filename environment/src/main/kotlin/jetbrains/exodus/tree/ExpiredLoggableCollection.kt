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
package jetbrains.exodus.tree

import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.dataStructures.IntArrayList
import jetbrains.exodus.core.dataStructures.LongArrayList
import jetbrains.exodus.log.Loggable

class ExpiredLoggableCollection(private val parent: ExpiredLoggableCollection? = null,
                                private val addresses: LongArrayList = LongArrayList(),
                                private val lengths: IntArrayList = IntArrayList(),
                                private val calcUtilizationFromScratch: Boolean = false) {

    val size: Int get() = lengths.size() + (parent?.size ?: 0)

    val fromScratch: Boolean get() = calcUtilizationFromScratch

    fun add(loggable: Loggable) = add(loggable.address, loggable.length())

    fun add(address: Long, length: Int) {
        addresses.add(address)
        lengths.add(length)
    }

    fun trimToSize(): ExpiredLoggableCollection {
        addresses.trimToSize()
        lengths.trimToSize()
        return this
    }

    fun mergeWith(parent: ExpiredLoggableCollection): ExpiredLoggableCollection {
        if (fromScratch) return this
        if (parent.fromScratch) return parent
        return this.parent?.let {
            parent.parent?.let {
                throw ExodusException("Can't merge 2 ExpiredLoggableCollections with both non-trivial parents")
            }
            ExpiredLoggableCollection(this, parent.addresses, parent.lengths)
        } ?: ExpiredLoggableCollection(parent, addresses, lengths)
    }

    fun forEach(action: (Long, Int) -> Unit): ExpiredLoggableCollection? {
        for (i in 0 until lengths.size()) {
            action(addresses[i], lengths[i])
        }
        return parent
    }

    override fun toString() = "Expired $size loggables"

    companion object {
        @JvmStatic
        val EMPTY = ExpiredLoggableCollection()

        @JvmStatic
        val FROM_SCRATCH = ExpiredLoggableCollection(calcUtilizationFromScratch = true)
    }
}