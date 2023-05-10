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
package jetbrains.exodus.tree.btree

import jetbrains.exodus.*
import jetbrains.exodus.ByteIterator
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.log.*

abstract class BasePageImmutable : BasePage {
    @JvmField
    protected val data: ByteIterableWithAddress

    @JvmField
    var keyAddressLen: Byte = 0

    private var _minKey: ILeafNode? = null
    override fun getMinKey(): ILeafNode {
        var minKey = _minKey
        if (minKey != null) {
            return minKey
        }

        minKey = super.getMinKey()
        this._minKey = minKey
        return minKey
    }

    private var _maxKey: ILeafNode? = null
    override fun getMaxKey(): ILeafNode {
        var maxKey = _maxKey
        if (maxKey != null) {
            return maxKey
        }

        maxKey = super.getMaxKey()
        this._maxKey = maxKey

        return maxKey
    }


    @JvmField
    protected val log: Log

    @JvmField
    protected val page: ByteArray?

    private val dataOffset: Int
    private val formatWithHashCodeIsUsed: Boolean

    /**
     * Create empty page
     *
     * @param tree tree which the page belongs to
     */
    constructor(tree: BTreeBase) : super(tree) {
        data = ByteIterableWithAddress.EMPTY
        size = 0
        log = tree.log
        formatWithHashCodeIsUsed = log.formatWithHashCodeIsUsed
        dataOffset = 0
        page = ArrayByteIterable.EMPTY_BYTES
    }

    /**
     * Create page and load size and key address length
     *
     * @param tree tree which the page belongs to
     * @param data binary data to load the page from.
     */
    constructor(
        tree: BTreeBase, data: ByteIterableWithAddress,
        loggableInsideSinglePage: Boolean
    ) : super(tree) {
        log = tree.log
        val it = data.iterator()
        size = it.compressedUnsignedInt shr 1
        this.data = init(data, it)
        formatWithHashCodeIsUsed = log.formatWithHashCodeIsUsed
        if (loggableInsideSinglePage) {
            page = this.data.baseBytes
            dataOffset = this.data.baseOffset()
        } else {
            page = null
            dataOffset = -1
        }
    }

    /**
     * Create page of specified size and load key address length
     *
     * @param tree tree which the page belongs to
     * @param data source iterator
     * @param size computed size
     */
    constructor(
        tree: BTreeBase,
        data: ByteIterableWithAddress, size: Int,
        loggableInsideSinglePage: Boolean
    ) : super(tree) {
        log = tree.log

        this.size = size
        val it = data.iterator()
        this.data = init(data, it)
        formatWithHashCodeIsUsed = log.formatWithHashCodeIsUsed

        if (loggableInsideSinglePage) {
            page = this.data.baseBytes
            dataOffset = this.data.baseOffset()
        } else {
            page = null
            dataOffset = -1
        }
    }

    private fun init(data: ByteIterableWithAddress, itr: ByteIteratorWithAddress): ByteIterableWithAddress {
        val result: ByteIterableWithAddress
        if (size > 0) {
            val next = itr.next().toInt()
            result = data.cloneWithAddressAndLength(itr.address, itr.available())
            loadAddressLengths(next, itr)
        } else {
            result = data.cloneWithAddressAndLength(itr.address, itr.available())
        }
        return result
    }

    override fun getDataAddress(): Long = data.getDataAddress()
    fun getDataIterator(): ByteIterator =
        if (data.getDataAddress() == Loggable.NULL_ADDRESS) ByteIterable.EMPTY_ITERATOR else data.iterator()

    protected open fun loadAddressLengths(length: Int, it: ByteIterator) {
        keyAddressLen = length.toByte()
        checkAddressLength(keyAddressLen)
    }

    override fun getKeyAddress(index: Int): Long {
        if (getDataAddress() == Loggable.NULL_ADDRESS) {
            return Loggable.NULL_ADDRESS
        }
        return if (page != null) {
            getLong(index * keyAddressLen, keyAddressLen.toInt())
        } else data.nextLong(index * keyAddressLen, keyAddressLen.toInt())
    }

    protected fun getLong(offset: Int, length: Int): Long {
        var input = offset
        input += dataOffset
        var result: Long = 0
        val page = page!!

        for (i in 0 until length) {
            result = (result shl 8) + (page[input + i].toInt() and 0xff)
        }

        return result
    }

    override fun getKey(index: Int): BaseLeafNode {
        return tree.loadLeaf(getKeyAddress(index))
    }

    override fun isDupKey(index: Int): Boolean {
        return tree.isDupKey(getKeyAddress(index))
    }

    override fun isMutable(): Boolean = false

    override fun binarySearch(key: ByteIterable): Int {
        return binarySearch(key, 0)
    }

    override fun binarySearch(key: ByteIterable, low: Int, expectedAddress: Long): Int {
        return binarySearch(key, low)
    }

    override fun binarySearch(key: ByteIterable, low: Int): Int {
        if (getDataAddress() == Loggable.NULL_ADDRESS) {
            return -1
        }
        if (page != null) {
            return singePageBinarySearch(key, low)
        }
        return if (formatWithHashCodeIsUsed) {
            multiPageBinarySearch(key, low)
        } else {
            compatibleBinarySearch(key, low)
        }
    }

    private fun multiPageBinarySearch(key: ByteIterable, low: Int): Int {
        var currentLow = low
        val cachePageSize = log.cachePageSize
        val bytesPerAddress = keyAddressLen.toInt()
        val dataAddress = getDataAddress()
        var currentHigh = size - 1
        var leftAddress = -1L
        var leftPage: ByteArray? = null
        var rightAddress = -1L
        var rightPage: ByteArray? = null
        val adjustedPageSize = log.cachePageSize - BufferedDataWriter.HASH_CODE_SIZE
        val it = BinarySearchIterator(adjustedPageSize)
        while (currentLow <= currentHigh) {
            val mid = currentLow + currentHigh ushr 1
            val midAddress = log.adjustLoggableAddress(dataAddress, mid.toLong() * bytesPerAddress)
            val offset: Int = midAddress.toInt() and cachePageSize - 1
            it.offset = offset // cache page size is always a power of 2
            val pageAddress = midAddress - offset
            when (pageAddress) {
                leftAddress -> {
                    it.page = leftPage
                }

                rightAddress -> {
                    it.page = rightPage
                }

                else -> {
                    leftPage = log.getCachedPage(pageAddress)
                    it.page = leftPage
                    leftAddress = pageAddress
                }
            }
            val leafAddress: Long
            if (adjustedPageSize - offset < bytesPerAddress) {
                val nextPageAddress = pageAddress + cachePageSize
                if (rightAddress == nextPageAddress) {
                    it.nextPage = rightPage
                } else {
                    rightPage = log.getCachedPage(nextPageAddress)
                    it.nextPage = rightPage
                    rightAddress = nextPageAddress
                }
                leafAddress = it.asCompound().nextLong(bytesPerAddress)
            } else {
                leafAddress = it.nextLong(bytesPerAddress)
            }
            val cmp = tree.compareLeafToKey(leafAddress, key)
            if (cmp < 0) {
                currentLow = mid + 1
            } else if (cmp > 0) {
                currentHigh = mid - 1
            } else {
                // key found
                return mid
            }
        }
        // key not found
        return -(currentLow + 1)
    }

    private fun singePageBinarySearch(key: ByteIterable, low: Int): Int {
        var currentLow = low
        var currentHigh = size - 1
        val bytesPerAddress = keyAddressLen.toInt()
        while (currentLow <= currentHigh) {
            val mid = currentLow + currentHigh ushr 1
            val offset = mid * bytesPerAddress
            val leafAddress = getLong(offset, bytesPerAddress)
            val cmp = tree.compareLeafToKey(leafAddress, key)
            if (cmp < 0) {
                currentLow = mid + 1
            } else if (cmp > 0) {
                currentHigh = mid - 1
            } else {
                // key found
                return mid
            }
        }
        // key not found
        return -(currentLow + 1)
    }

    private fun compatibleBinarySearch(key: ByteIterable, low: Int): Int {
        var currentLow = low
        val cachePageSize = log.cachePageSize
        val bytesPerAddress = keyAddressLen.toInt()
        val dataAddress = getDataAddress()
        var currentHigh = size - 1
        var leftAddress = -1L
        var leftPage: ByteArray? = null
        var rightAddress = -1L
        var rightPage: ByteArray? = null
        while (currentLow <= currentHigh) {
            val mid = currentLow + currentHigh ushr 1
            val midAddress = log.adjustLoggableAddress(dataAddress, mid.toLong() * bytesPerAddress)
            val adjustedPageSize: Int = if (formatWithHashCodeIsUsed) {
                cachePageSize - BufferedDataWriter.HASH_CODE_SIZE
            } else {
                cachePageSize
            }
            val it = BinarySearchIterator(adjustedPageSize)
            val offset: Int = midAddress.toInt() and cachePageSize - 1
            it.offset = offset // cache page size is always a power of 2
            val pageAddress = midAddress - offset
            when (pageAddress) {
                leftAddress -> {
                    it.page = leftPage
                }

                rightAddress -> {
                    it.page = rightPage
                }

                else -> {
                    leftPage = log.getCachedPage(pageAddress)
                    it.page = leftPage
                    leftAddress = pageAddress
                }
            }
            val leafAddress: Long
            if (adjustedPageSize - offset < bytesPerAddress) {
                val nextPageAddress = pageAddress + cachePageSize
                if (rightAddress == nextPageAddress) {
                    it.nextPage = rightPage
                } else {
                    rightPage = log.getCachedPage(nextPageAddress)
                    it.nextPage = rightPage
                    rightAddress = nextPageAddress
                }
                leafAddress = it.asCompound().nextLong(bytesPerAddress)
            } else {
                leafAddress = it.nextLong(bytesPerAddress)
            }
            val cmp = tree.compareLeafToKey(leafAddress, key)
            if (cmp < 0) {
                currentLow = mid + 1
            } else if (cmp > 0) {
                currentHigh = mid - 1
            } else {
                // key found
                return mid
            }
        }

        // key not found
        return -(currentLow + 1)
    }

    private class BinarySearchIterator(private val adjustedPageSize: Int) : ByteIterator {
        @JvmField
        var page: ByteArray? = null

        @JvmField
        var nextPage: ByteArray? = null

        @JvmField
        var offset = 0
        fun asCompound(): CompoundByteIteratorBase {
            return object : CompoundByteIteratorBase(this) {
                override fun nextIterator(): ByteIterator {
                    page = nextPage
                    offset = 0
                    return this@BinarySearchIterator
                }
            }
        }

        override fun hasNext(): Boolean {
            return offset < adjustedPageSize
        }

        override fun next(): Byte {
            return page!![offset++]
        }

        override fun skip(bytes: Long): Long {
            throw UnsupportedOperationException()
        }

        override fun nextLong(length: Int): Long {
            return LongBinding.entryToUnsignedLong(page, offset, length)
        }
    }

    companion object {
        fun checkAddressLength(addressLen: Byte) {
            if (addressLen < 0 || addressLen > 8) {
                throw ExodusException("Invalid length of address: $addressLen")
            }
        }

        fun doReclaim(context: BTreeReclaimTraverser) {
            val node = context.currentNode.getMutableCopy(context.mainTree)
            context.wasReclaim = true
            context.setPage(node)
            context.popAndMutate()
        }
    }
}
