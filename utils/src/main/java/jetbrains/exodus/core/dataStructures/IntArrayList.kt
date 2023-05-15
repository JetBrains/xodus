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

class IntArrayList @JvmOverloads constructor(initialCapacity: Int = 4) : Cloneable {
    private var data: IntArray
    private var size = 0

    init {
        data = IntArray(initialCapacity)
    }

    fun trimToSize() {
        val oldCapacity = data.size
        if (size < oldCapacity) {
            data = Arrays.copyOf(data, size)
        }
    }

    fun ensureCapacity(minCapacity: Int) {
        var oldCapacity = data.size
        if (minCapacity > oldCapacity) {
            if (oldCapacity == 0) {
                oldCapacity = 1
            }
            var newCapacity = (oldCapacity shl 3) / 5 + 1
            if (newCapacity < minCapacity) {
                newCapacity = minCapacity
            }
            data = Arrays.copyOf(data, newCapacity)
        }
    }

    var capacity: Int
        get() = data.size
        set(capacity) {
            data = IntArray(capacity)
        }

    fun size(): Int {
        return size
    }

    val isEmpty: Boolean
        get() = size == 0

    operator fun contains(element: Int): Boolean {
        return indexOf(element) >= 0
    }

    fun indexOf(element: Int): Int {
        return indexOf(data, size, element)
    }

    fun lastIndexOf(element: Int): Int {
        for (i in size - 1 downTo 0) {
            if (element == data[i]) {
                return i
            }
        }
        return -1
    }

    @Throws(CloneNotSupportedException::class)
    public override fun clone(): Any {
        val v = super.clone() as IntArrayList
        v.data = IntArray(size)
        System.arraycopy(data, 0, v.data, 0, size)
        return v
    }

    fun toArray(): IntArray {
        val result = IntArray(size)
        System.arraycopy(data, 0, result, 0, size)
        return result
    }

    fun toArray(a: IntArray): IntArray {
        var a = a
        if (a.size < size) {
            a = IntArray(size)
        }
        System.arraycopy(data, 0, a, 0, size)
        return a
    }

    operator fun get(index: Int): Int {
        checkRange(index)
        return data[index]
    }

    operator fun set(index: Int, element: Int): Int {
        checkRange(index)
        val oldValue = data[index]
        data[index] = element
        return oldValue
    }

    fun add(element: Int) {
        ensureCapacity(size + 1)
        data[size++] = element
    }

    fun add(index: Int, element: Int) {
        if (index > size || index < 0) {
            throw IndexOutOfBoundsException("Index: $index, Size: $size")
        }
        ensureCapacity(size + 1)
        System.arraycopy(data, index, data, index + 1, size - index)
        data[index] = element
        size++
    }

    fun remove(index: Int): Int {
        checkRange(index)
        val oldValue = data[index]
        val numMoved = size - index - 1
        if (numMoved > 0) {
            System.arraycopy(data, index + 1, data, index, numMoved)
        }
        size--
        return oldValue
    }

    fun clear() {
        size = 0
    }

    protected fun removeRange(fromIndex: Int, toIndex: Int) {
        val numMoved = size - toIndex
        System.arraycopy(data, toIndex, data, fromIndex, numMoved)
        size -= toIndex - fromIndex
    }

    private fun checkRange(index: Int) {
        if (index >= size || index < 0) {
            throw IndexOutOfBoundsException("Index: $index, Size: $size")
        }
    }

    companion object {
        fun indexOf(array: IntArray, element: Int): Int {
            return indexOf(array, array.size, element)
        }

        fun indexOf(array: IntArray, size: Int, element: Int): Int {
            for (i in 0 until size) {
                if (element == array[i]) {
                    return i
                }
            }
            return -1
        }
    }
}
