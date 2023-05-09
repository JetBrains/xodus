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
package jetbrains.exodus.tree.patricia

import java.util.NoSuchElementException

internal class ChildReferenceSet : Iterable<ChildReference> {
    private var refs: Array<ChildReference?>? = null
    fun clear(capacity: Int) {
        refs = if (capacity == 0) null else arrayOfNulls(capacity)
    }

    fun size(): Int {
        return if (refs == null) 0 else refs!!.size
    }

    fun isEmpty(): Boolean = size() == 0

    operator fun get(b: Byte): ChildReference? {
        val index = searchFor(b)
        return if (index < 0) null else refs!![index]
    }

    fun getRight(): ChildReference? {
        val size = size()
        return if (size > 0) refs!![size - 1] else null
    }

    fun searchFor(b: Byte): Int {
        val refs = refs
        val key = b.toInt() and 0xff
        var low = 0
        var high = size() - 1
        while (low <= high) {
            val mid = low + high + 1 ushr 1
            val midRef = refs!![mid]
            val cmp = if (midRef == null) 1 else (midRef.firstByte.toInt() and 0xff) - key
            if (cmp < 0) {
                low = mid + 1
            } else if (cmp > 0) {
                high = mid - 1
            } else {
                return mid
            }
        }
        return -low - 1
    }

    fun referenceAt(index: Int): ChildReference {
        return refs!![index]!!
    }

    fun putRight(ref: ChildReference) {
        val size = size()
        ensureCapacity(size + 1, size)
        refs!![size] = ref
    }

    fun insertAt(index: Int, ref: ChildReferenceMutable) {
        ensureCapacity(size() + 1, index)
        refs!![index] = ref
    }

    fun setAt(index: Int, ref: ChildReference) {
        refs!![index] = ref
    }

    fun remove(b: Byte): Boolean {
        val index = searchFor(b)
        if (index < 0) {
            return false
        }
        val size = size()
        if (size == 1) {
            refs = null
        } else {
            val refs = refs!!

            val refsToCopy = size - (index + 1)
            if (refsToCopy > 0) {
                refs.copyInto(refs, index, index + 1)
            }

            this.refs = refs.copyOf(size - 1)
        }
        return true
    }

    override fun iterator(): ChildReferenceIterator {
        return iterator(-1)
    }

    fun iterator(index: Int): ChildReferenceIterator {
        return ChildReferenceIterator(this, index)
    }

    private fun ensureCapacity(capacity: Int, insertPos: Int) {
        val refs = refs
        if (refs == null) {
            this.refs = arrayOfNulls(capacity)
        } else {
            val length = refs.size
            if (length >= capacity) {
                if (insertPos < length - 1) {
                    System.arraycopy(refs, insertPos, refs, insertPos + 1, length - insertPos - 1)
                }
            } else {
                this.refs = arrayOfNulls<ChildReference>(length.coerceAtLeast(capacity)).also {
                    refs.copyInto(it, 0, 0, insertPos)
                    refs.copyInto(it, insertPos + 1, insertPos, length)
                }
            }
        }
    }

    class ChildReferenceIterator(set: ChildReferenceSet, var index: Int) : MutableIterator<ChildReference> {
        private val refs: Array<ChildReference?>?
        private val size: Int

        init {
            refs = set.refs
            size = set.size()
        }

        override fun hasNext(): Boolean {
            return index < size - 1
        }

        override fun next(): ChildReference {
            var ref: ChildReference?
            var i = index
            do {
                if (++i >= size) {
                    throw NoSuchElementException()
                }
                ref = refs!![i]
            } while (ref == null)
            index = i
            return ref
        }

        fun prev(): ChildReference {
            var ref: ChildReference?
            var i = index
            do {
                if (--i < 0) {
                    throw NoSuchElementException()
                }
                ref = refs!![i]
            } while (ref == null)
            index = i
            return ref
        }

        override fun remove() {
            throw UnsupportedOperationException()
        }

        fun currentRef(): ChildReference? {
            val i = index
            return if (i in 0 until size) refs!![i] else null
        }
    }
}
