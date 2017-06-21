package jetbrains.exodus.entitystore.iterate.util

import jetbrains.exodus.core.dataStructures.hash.LongIterator

class LongArrayIterator @JvmOverloads constructor(val array: LongArray, val size: Int = array.size) : LongIterator {
    private var index = 0
    override fun hasNext() = index < size
    override fun next() = nextLong()
    override fun nextLong() = array[index++]

    override fun remove() = throw UnsupportedOperationException()
}
