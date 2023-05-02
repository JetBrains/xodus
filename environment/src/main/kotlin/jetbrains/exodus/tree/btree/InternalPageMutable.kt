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
import jetbrains.exodus.bindings.CompressedUnsignedLongArrayByteIterable
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getIterable
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.tree.Dumpable

import java.io.PrintStream
import kotlin.math.max

class InternalPageMutable : BasePageMutable {
    internal lateinit var children: Array<BasePageMutable?>
    private lateinit var childrenAddresses: LongArray

    internal constructor(tree: BTreeMutable, page: InternalPage) : super(tree, page)
    private constructor(page: InternalPageMutable, from: Int, length: Int) : super(page.tree as BTreeMutable) {
        val bp = balancePolicy
        createChildren(
            max(
                (length and 0x7ffffffe) + 2 /* we should have at least one more place to insert a key */,
                if ((tree as BTreeMutable).isDup) bp.dupPageMaxSize else bp.pageMaxSize
            )
        )
        System.arraycopy(page.keys, from, keys, 0, length)
        System.arraycopy(page.keysAddresses, from, keysAddresses, 0, length)
        System.arraycopy(page.children, from, children, 0, length)
        System.arraycopy(page.childrenAddresses, from, childrenAddresses, 0, length)
        size = length
    }

    internal constructor(tree: BTreeMutable, page1: BasePageMutable, page2: BasePageMutable) : super(tree) {
        val bp = balancePolicy
        createChildren(if (tree.isDup) bp.dupPageMaxSize else bp.pageMaxSize)
        set(0, page1.minKey, page1)
        set(1, page2.minKey, page2)
        size = 2
    }

    override fun load(it: ByteIterator, keyAddressLen: Int) {
        super.load(it, keyAddressLen)
        CompressedUnsignedLongArrayByteIterable.loadLongs(childrenAddresses, it, size)
    }

    override val isBottom: Boolean
        get() = false

    override fun createChildren(max: Int) {
        super.createChildren(max)
        children = arrayOfNulls(max)
        childrenAddresses = LongArray(max)
    }

    override fun getChildAddress(index: Int): Long {
        return childrenAddresses[index]
    }

    override fun getChild(index: Int): BasePage {
        return children[index] ?: run {
            tree.loadPage(childrenAddresses[index])
        }
    }

    override fun childExists(key: ByteIterable, pageAddress: Long): Boolean {
        val index: Int = InternalPage.binarySearchGuessUnsafe(this, key)
        return index >= 0 && (childrenAddresses[index] == pageAddress || getChild(index).childExists(key, pageAddress))
    }

    override val type: Byte
        get() = (tree as BTreeMutable).internalPageType

    private fun getMutableChild(index: Int): BasePageMutable {
        if (index >= size) {
            throw ArrayIndexOutOfBoundsException("$index >= $size")
        }
        val tree = tree as BTreeMutable
        return children[index] ?: run {
            val childAddress = childrenAddresses[index]
            tree.addExpiredLoggable(childAddress)
            val child = tree.loadPage(childAddress).getMutableCopy(tree)
            children[index] = child
            // loaded mutable page will be changed and must be saved
            childrenAddresses[index] = Loggable.NULL_ADDRESS
            child
        }

    }

    override fun setMutableChild(index: Int, child: BasePageMutable) {
        if (children[index] !== child) {
            val key = child.keys[0]
            if (key != null) { // first key is mutable ==> changed, no merges or reclaims allowed
                keys[index] = key
                keysAddresses[index] = key.address
            }
            children[index] = child
            (tree as BTreeMutable).addExpiredLoggable(childrenAddresses[index])
            childrenAddresses[index] = Loggable.NULL_ADDRESS
        }
    }

    override fun put(
        key: ByteIterable,
        value: ByteIterable,
        overwrite: Boolean,
        result: BooleanArray
    ): BasePageMutable? {
        var pos = binarySearch(key)
        if (pos >= 0 && !overwrite) {
            // key found and overwrite is not possible - error
            return null
        }
        if (pos < 0) {
            pos = -pos - 2
            // if insert after last - set to last
            if (pos < 0) pos = 0
        }
        val tree = tree as BTreeMutable
        val child = getChild(pos).getMutableCopy(tree)
        val newChild = child.put(key, value, overwrite, result)
        // change min key for child
        if (result[0]) {
            tree.addExpiredLoggable(childrenAddresses[pos])
            set(pos, child.minKey, child)
            if (newChild != null) {
                return insertAt(pos + 1, newChild.minKey, newChild)
            }
        }
        return null
    }

    override fun putRight(key: ByteIterable, value: ByteIterable): BasePageMutable? {
        val pos = size - 1
        val tree = tree as BTreeMutable
        val child = getChild(pos).getMutableCopy(tree)
        val newChild = child.putRight(key, value)
        // change min key for child
        tree.addExpiredLoggable(childrenAddresses[pos])
        set(pos, child.minKey, child)
        return if (newChild != null) {
            insertAt(pos + 1, newChild.minKey, newChild)
        } else null
    }

    override fun set(pos: Int, key: ILeafNode, child: BasePageMutable?) {
        super.set(pos, key, child)
        // remember mutable page and reset address to save it later
        children[pos] = child!!
        childrenAddresses[pos] = Loggable.NULL_ADDRESS
    }

    override fun copyChildren(from: Int, to: Int) {
        if (from >= size) return
        super.copyChildren(from, to)
        System.arraycopy(children, from, children, to, size - from)
        System.arraycopy(childrenAddresses, from, childrenAddresses, to, size - from)
    }

    override fun decrementSize(value: Int) {
        val initialSize = size
        super.decrementSize(value)
        for (i in size until initialSize) {
            children[i] = null
            childrenAddresses[i] = 0L
        }
    }

    override fun split(from: Int, length: Int): BasePageMutable {
        val result = InternalPageMutable(this, from, length)
        decrementSize(length)
        return result
    }

    override fun get(key: ByteIterable): ILeafNode? {
        return InternalPage[key, this]
    }

    override fun find(
        stack: BTreeTraverser,
        depth: Int,
        key: ByteIterable,
        value: ByteIterable?,
        equalOrNext: Boolean
    ): ILeafNode? {
        return InternalPage.find(stack, depth, key, value, equalOrNext, this)
    }

    override fun keyExists(key: ByteIterable): Boolean {
        return InternalPage.keyExists(key, this)
    }

    override fun exists(key: ByteIterable, value: ByteIterable): Boolean {
        return InternalPage.exists(key, value, this)
    }

    override val bottomPagesCount: Long
        get() {
            var result: Long = 0
            for (i in 0 until size) {
                result += getChild(i).bottomPagesCount
            }
            return result
        }

    override fun saveChildren(): ReclaimFlag {
        // save children to get their addresses
        var result = ReclaimFlag.RECLAIM
        for (i in 0 until size) {
            if (childrenAddresses[i] == Loggable.NULL_ADDRESS) {
                val child = children[i]!!

                childrenAddresses[i] = child.save()
                keysAddresses[i] = child.keysAddresses[0]
                result = ReclaimFlag.PRESERVE
            }
        }
        return result
    }

    override fun getByteIterables(flag: ReclaimFlag): Array<ByteIterable> {
        return arrayOf(
            getIterable(((size shl 1) + flag.value).toLong()),
            CompressedUnsignedLongArrayByteIterable.getIterable(keysAddresses, size),
            CompressedUnsignedLongArrayByteIterable.getIterable(childrenAddresses, size)
        )
    }

    override fun toString(): String {
        return "Internal* [$size]"
    }

    override fun dump(out: PrintStream, level: Int, renderer: Dumpable.ToString?) {
        InternalPage.dump(out, level, renderer, this)
    }

    override fun mergeWithRight(page: BasePageMutable) {
        val internalPage = page as InternalPageMutable
        val newPageSize = size + internalPage.size
        if (newPageSize >= keys.size) {
            val newArraySize = (newPageSize and 0x7ffffffe) + 2
            keys = keys.copyOf(newArraySize)
            keysAddresses = keysAddresses.copyOf(newArraySize)
            children = children.copyOf(newArraySize)
            childrenAddresses = childrenAddresses.copyOf(newArraySize)
        }
        System.arraycopy(internalPage.keys, 0, keys, size, internalPage.size)
        System.arraycopy(internalPage.keysAddresses, 0, keysAddresses, size, internalPage.size)
        System.arraycopy(internalPage.children, 0, children, size, internalPage.size)
        System.arraycopy(internalPage.childrenAddresses, 0, childrenAddresses, size, internalPage.size)
        size += internalPage.size
    }

    override fun mergeWithLeft(page: BasePageMutable) {
        val internalPage = page as InternalPageMutable
        internalPage.mergeWithRight(this)
        keys = internalPage.keys
        keysAddresses = internalPage.keysAddresses
        children = internalPage.children
        childrenAddresses = internalPage.childrenAddresses
        size = internalPage.size
    }

    private fun removeChild(pos: Int) {
        copyChildren(pos + 1, pos)
        decrementSize(1)
    }

    override fun delete(key: ByteIterable, value: ByteIterable?): Boolean {
        val pos: Int = InternalPage.binarySearchGuess(this, key)
        val child = getMutableChild(pos)
        if (!child.delete(key, value)) {
            return false
        }
        // if first element was removed in child, then update min key
        val childSize = child.size
        if (childSize > 0) {
            set(pos, child.minKey, child)
        }
        val balancePolicy = balancePolicy
        if (pos > 0) {
            val left = getChild(pos - 1)
            if (balancePolicy.needMerge(left, child)) {
                // merge child into left sibling
                // reget mutable left
                getMutableChild(pos - 1).mergeWithRight(child)
                removeChild(pos)
            }
        } else if (pos + 1 < size) {
            val right = getChild(pos + 1)
            if (balancePolicy.needMerge(child, right)) {
                // merge child with right sibling
                val mutableRight = getMutableChild(pos + 1)
                mutableRight.mergeWithLeft(child)
                removeChild(pos)
                // change key for link to right
                set(pos, mutableRight.minKey, mutableRight)
            }
        } else if (childSize == 0) {
            removeChild(pos)
        }
        return true
    }

    override fun mergeWithChildren(): BasePageMutable {
        var result: BasePageMutable = this
        while (!result.isBottom && result.size == 1) {
            result = (result as InternalPageMutable).getMutableChild(0)
        }
        return result
    }
}
