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
package jetbrains.exodus.core.dataStructures

import java.util.*

class LongArrayList @JvmOverloads constructor(initialCapacity: Int = 4) : Cloneable {
    var instantArray: LongArray
        private set
    private var size = 0

    init {
        instantArray = LongArray(initialCapacity)
    }

    fun trimToSize() {
        val oldCapacity = instantArray.size
        if (size < oldCapacity) {
            instantArray = Arrays.copyOf(instantArray, size)
        }
    }

    fun ensureCapacity(minCapacity: Int) {
        var oldCapacity = instantArray.size
        if (minCapacity > oldCapacity) {
            if (oldCapacity == 0) {
                oldCapacity = 1
            }
            var newCapacity = (oldCapacity shl 3) / 5 + 1
            if (newCapacity < minCapacity) {
                newCapacity = minCapacity
            }
            instantArray = Arrays.copyOf(instantArray, newCapacity)
        }
    }

    var capacity: Int
        get() = instantArray.size
        set(capacity) {
            instantArray = LongArray(capacity)
        }

    fun size(): Int {
        return size
    }

    val isEmpty: Boolean
        get() = size == 0

    operator fun contains(element: Long): Boolean {
        return indexOf(element) >= 0
    }

    fun indexOf(element: Long): Int {
        return indexOf(instantArray, size, element)
    }

    fun lastIndexOf(element: Long): Int {
        for (i in size - 1 downTo 0) {
            if (element == instantArray[i]) return i
        }
        return -1
    }

    fun sort() {
        Arrays.sort(instantArray, 0, size)
    }

    @Throws(CloneNotSupportedException::class)
    public override fun clone(): Any {
        val v = super.clone() as LongArrayList
        v.instantArray = LongArray(size)
        System.arraycopy(instantArray, 0, v.instantArray, 0, size)
        return v
    }

    fun toArray(): LongArray {
        val result = LongArray(size)
        System.arraycopy(instantArray, 0, result, 0, size)
        return result
    }

    fun toArray(a: LongArray): LongArray {
        var a = a
        if (a.size < size) {
            a = LongArray(size)
        }
        System.arraycopy(instantArray, 0, a, 0, size)
        return a
    }

    operator fun get(index: Int): Long {
        checkRange(index)
        return instantArray[index]
    }

    operator fun set(index: Int, element: Long): Long {
        checkRange(index)
        val oldValue = instantArray[index]
        instantArray[index] = element
        return oldValue
    }

    fun add(element: Long) {
        ensureCapacity(size + 1)
        instantArray[size++] = element
    }

    fun add(index: Int, element: Long) {
        if (index > size || index < 0) {
            throw IndexOutOfBoundsException("Index: $index, Size: $size")
        }
        ensureCapacity(size + 1)
        System.arraycopy(instantArray, index, instantArray, index + 1, size - index)
        instantArray[index] = element
        size++
    }

    fun remove(index: Int): Long {
        checkRange(index)
        val oldValue = instantArray[index]
        val numMoved = size - index - 1
        if (numMoved > 0) {
            System.arraycopy(instantArray, index + 1, instantArray, index, numMoved)
        }
        size--
        return oldValue
    }

    fun clear() {
        size = 0
    }

    protected fun removeRange(fromIndex: Int, toIndex: Int) {
        val numMoved = size - toIndex
        System.arraycopy(instantArray, toIndex, instantArray, fromIndex, numMoved)
        size -= toIndex - fromIndex
    }

    private fun checkRange(index: Int) {
        if (index >= size || index < 0) {
            throw IndexOutOfBoundsException("Index: $index, Size: $size")
        }
    }

    companion object {
        @JvmStatic
        fun indexOf(array: LongArray, element: Long): Int {
            return indexOf(array, array.size, element)
        }

        fun indexOf(array: LongArray, size: Int, element: Long): Int {
            for (i in 0 until size) {
                if (element == array[i]) return i
            }
            return -1
        }
    }
}
