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

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.ByteIterator
import jetbrains.exodus.CompoundByteIterable
import jetbrains.exodus.ExodusException
import jetbrains.exodus.bindings.CompressedUnsignedLongArrayByteIterable
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getIterable
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.log.TooBigLoggableException
import jetbrains.exodus.tree.MutableTreeRoot
import kotlin.math.max

abstract class BasePageMutable : BasePage, MutableTreeRoot {
    @JvmField
    internal var keys: Array<BaseLeafNodeMutable?>? = null

    @JvmField
    internal var keysAddresses: LongArray? = null

    protected constructor(tree: BTreeMutable) : super(tree)
    protected constructor(tree: BTreeMutable, page: BasePageImmutable) : super(tree) {
        size = page.size
        val bp = getBalancePolicy()
        @Suppress("LeakingThis")
        createChildren(max(page.size, if (tree.isDup()) bp.dupPageMaxSize else bp.pageMaxSize))
        if (size > 0) {
            @Suppress("LeakingThis")
            load(page.getDataIterator(), page.keyAddressLen.toInt())
        }
    }

    protected open fun load(it: ByteIterator, keyAddressLen: Int) {
        // create array with max size for key addresses
        CompressedUnsignedLongArrayByteIterable.loadLongs(keysAddresses, it, size, keyAddressLen)
    }

    override fun getMutableCopy(treeMutable: BTreeMutable): BasePageMutable {
        return this
    }

    override fun getDataAddress(): Long = Loggable.NULL_ADDRESS

    protected open fun createChildren(max: Int) {
        keys = arrayOfNulls(max)
        keysAddresses = LongArray(max)
    }

    /**
     * Deletes key/value pair. If key corresponds to several duplicates, remove all of them.
     *
     * @param key   key to delete
     * @param value value to delete
     * @return true iff succeeded
     */
    abstract fun delete(key: ByteIterable, value: ByteIterable?): Boolean

    /**
     * Insert or update value in tree.
     * If tree supports duplicates and key exists, inserts after existing key
     *
     * @param key       key to put
     * @param value     value to put
     * @param overwrite true if existing value by the key should be overwritten
     * @param result    false if key exists, overwite is false and tree is not support duplicates
     */
    abstract fun put(key: ByteIterable, value: ByteIterable, overwrite: Boolean, result: BooleanArray): BasePageMutable?
    abstract fun putRight(key: ByteIterable, value: ByteIterable): BasePageMutable?

    /**
     * Serialize page data
     *
     * @return serialized data
     */
    fun getData(): ByteIterable = CompoundByteIterable(getByteIterables(saveChildren()))

    protected abstract fun saveChildren(): ReclaimFlag
    protected abstract fun getByteIterables(flag: ReclaimFlag): Array<ByteIterable>

    /**
     * Save page to log
     *
     * @return address of this page after save
     */
    fun save(): Long {
        // save leaf nodes
        var flag = saveChildren()
        // save self. complementary to {@link load()}
        val type = getType()
        val tree = this.tree
        val structureId = tree.structureId
        val log = tree.log
        val expiredLoggables = (tree as BTreeMutable).getExpiredLoggables()
        if (flag == ReclaimFlag.PRESERVE) {
            // there is a chance to update the flag to RECLAIM
            if (log.writtenHighAddress % log.fileLengthBound == 0L) {
                // page will be exactly on file border
                flag = ReclaimFlag.RECLAIM
            } else {
                val iterables = getByteIterables(flag)
                var result = log.tryWrite(type, structureId, CompoundByteIterable(iterables), expiredLoggables)
                if (result < 0) {
                    iterables[0] = getIterable(
                        (size.toLong() shl 1) + ReclaimFlag.RECLAIM.value
                    )
                    result = log.writeContinuously(type, structureId, CompoundByteIterable(iterables), expiredLoggables)
                    if (result < 0) {
                        throw TooBigLoggableException()
                    }
                }
                return result
            }
        }
        return log.write(type, structureId, CompoundByteIterable(getByteIterables(flag)), expiredLoggables)
    }

    protected abstract fun getType(): Byte
    override fun getKeyAddress(index: Int): Long {
        return keysAddresses!![index]
    }

    override fun getKey(index: Int): BaseLeafNode {
        if (index >= size) {
            throw ArrayIndexOutOfBoundsException("$index >= $size")
        }
        val keys = keys!!
        return if (keys[index] == null) tree.loadLeaf(keysAddresses!![index]) else keys[index]!!
    }

    abstract fun setMutableChild(index: Int, child: BasePageMutable)
    protected fun getBalancePolicy(): BTreeBalancePolicy = tree.balancePolicy

    override fun isMutable(): Boolean = true

    protected fun insertAt(pos: Int, key: ILeafNode, child: BasePageMutable?): BasePageMutable? {
        return if (!getBalancePolicy().needSplit(this)) {
            insertDirectly(pos, key, child)
            null
        } else {
            val splitPos = getBalancePolicy().getSplitPos(this, pos)
            val sibling = split(splitPos, size - splitPos)
            if (pos >= splitPos) {
                // insert into right sibling
                sibling.insertDirectly(pos - splitPos, key, child)
            } else {
                // insert into self
                insertDirectly(pos, key, child)
            }
            sibling
        }
    }

    open operator fun set(pos: Int, key: ILeafNode, child: BasePageMutable?) {
        val keys = keys!!
        // do not remember immutable leaf, but only address
        if (key is BaseLeafNodeMutable) {
            keys[pos] = key
        } else {
            keys[pos] = null // forget previous mutable leaf
        }

        keysAddresses!![pos] = key.getAddress()
    }

    private fun insertDirectly(pos: Int, key: ILeafNode, child: BasePageMutable?) {
        if (pos < size) {
            copyChildren(pos, pos + 1)
        }
        size += 1
        set(pos, key, child)
    }

    protected open fun copyChildren(from: Int, to: Int) {
        if (from >= size) return
        val keys = keys!!
        val keysAddresses = keysAddresses!!

        System.arraycopy(keys, from, keys, to, size - from)
        System.arraycopy(keysAddresses, from, keysAddresses, to, size - from)
    }

    override fun binarySearch(key: ByteIterable): Int {
        return binarySearch(key, 0)
    }

    override fun binarySearch(key: ByteIterable, low: Int): Int {
        return binarySearch(this, key, low, size - 1)
    }

    override fun binarySearch(key: ByteIterable, low: Int, expectedAddress: Long): Int {
        // searching for the address linearly in keyAdresses seems to be quite cheap
        // if we find it we don't have to perform binary search with several keys' loadings
        val size = this.size
        val keysAddresses = keysAddresses!!

        for (i in low until size) {
            if (keysAddresses[i] == expectedAddress && getKey(i).compareKeyTo(key) == 0) {
                return i
            }
        }
        return binarySearch(this, key, low, size - 1)
    }

    protected open fun decrementSize(value: Int) {
        if (size < value) {
            throw ExodusException("Can't decrease BTree page size $size on $value")
        }
        val initialSize = size
        size -= value
        val keys = keys!!
        val keysAddresses = keysAddresses!!

        for (i in size until initialSize) {
            keys[i] = null
            keysAddresses[i] = 0L
        }
    }

    protected abstract fun split(from: Int, length: Int): BasePageMutable
    abstract fun mergeWithChildren(): BasePageMutable
    abstract fun mergeWithRight(page: BasePageMutable)
    abstract fun mergeWithLeft(page: BasePageMutable)
    protected enum class ReclaimFlag(@JvmField val value: Int) {
        PRESERVE(0),
        RECLAIM(1)
    }

    companion object {
        protected fun binarySearch(
            page: BasePage,
            key: ByteIterable,
            low: Int, high: Int
        ): Int {
            var currentLow = low
            var currentHigh = high
            while (currentLow <= currentHigh) {
                val mid = currentLow + currentHigh + 1 ushr 1
                val midKey: ILeafNode = page.getKey(mid)
                val cmp = midKey.compareKeyTo(key)
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
    }
}
