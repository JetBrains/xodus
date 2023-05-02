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
import jetbrains.exodus.bindings.CompressedUnsignedLongArrayByteIterable
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getIterable
import jetbrains.exodus.log.Loggable
import jetbrains.exodus.tree.Dumpable
import java.io.PrintStream
import kotlin.math.max

internal class BottomPageMutable : BasePageMutable {
    constructor(tree: BTreeMutable, page: BottomPage) : super(tree, page)
    private constructor(page: BottomPageMutable, from: Int, length: Int) : super(page.tree as BTreeMutable) {
        val bp = balancePolicy
        val max = max(
            (length and 0x7ffffffe) + 2 /* we should have at least one more place to insert a key */,
            if ((tree as BTreeMutable).isDup) bp.dupPageMaxSize else bp.pageMaxSize
        )
        keys = arrayOfNulls(max)
        keysAddresses = LongArray(max)
        System.arraycopy(page.keys, from, keys, 0, length)
        System.arraycopy(page.keysAddresses, from, keysAddresses, 0, length)
        size = length
    }

    override val isBottom: Boolean
        get() = true

    override fun getChildAddress(index: Int): Long {
        return keysAddresses[index]
    }

    override fun put(
        key: ByteIterable,
        value: ByteIterable,
        overwrite: Boolean,
        result: BooleanArray
    ): BasePageMutable? {
        val tree = tree as BTreeMutable
        var pos = binarySearch(key)
        if (pos >= 0) {
            if (overwrite) {
                // key found
                val ln: ILeafNode = getKey(pos)
                if (tree.allowsDuplicates) {
                    // overwrite for tree with duplicates means add new value to existing key
                    // manage sub-tree of duplicates
                    // ln may be mutable or immutable, with dups or without
                    val lnm: LeafNodeDupMutable = LeafNodeDupMutable.convert(ln, tree)
                    if (lnm.put(value)) {
                        tree.addExpiredLoggable(ln)
                        set(pos, lnm, null)
                        result[0] = true
                    }
                    // main tree size will be auto-incremented with some help from duplicates tree
                } else {
                    if (!ln.isDupLeaf) {
                        tree.addExpiredLoggable(ln)
                        set(pos, tree.createMutableLeaf(key, value), null)
                        // this should be always true in order to keep up with keysAddresses[pos] expiration
                        result[0] = true
                    }
                }
            }
            return null
        }

        // if found - insert at this position, else insert after found
        pos = -pos - 1
        val page = insertAt(pos, tree.createMutableLeaf(key, value), null)
        result[0] = true
        tree.incrementSize()
        return page
    }

    override fun putRight(key: ByteIterable, value: ByteIterable): BasePageMutable? {
        val tree = tree as BTreeMutable
        if (size > 0) {
            val pos = size - 1
            val ln = getKey(pos)
            val cmp = ln.compareKeyTo(key)
            require(cmp <= 0) { "Key must be greater" }
            if (cmp == 0) {
                return if (tree.allowsDuplicates) {
                    set(pos, LeafNodeDupMutable.convert(ln, tree).putRight(value), null)
                    tree.addExpiredLoggable(ln)
                    null
                } else {
                    throw IllegalArgumentException("Key must not be equal")
                }
            }
        }
        val page = insertAt(size, tree.createMutableLeaf(key, value), null)
        tree.incrementSize()
        return page
    }

    override fun split(from: Int, length: Int): BasePageMutable {
        val result = BottomPageMutable(this, from, length)
        decrementSize(length)
        return result
    }

    override val bottomPagesCount: Long
        get() = 1

    override fun get(key: ByteIterable): ILeafNode? {
        return BottomPage[key, this]
    }

    override fun find(
        stack: BTreeTraverser,
        depth: Int,
        key: ByteIterable,
        value: ByteIterable?,
        equalOrNext: Boolean
    ): ILeafNode? {
        return BottomPage.find(stack, depth, key, value, equalOrNext, this)
    }

    override fun keyExists(key: ByteIterable): Boolean {
        return BottomPage.keyExists(key, this)
    }

    override fun exists(key: ByteIterable, value: ByteIterable): Boolean {
        return BottomPage.exists(key, value, this)
    }

    override fun saveChildren(): ReclaimFlag {
        val tree = this.tree
        var result = ReclaimFlag.RECLAIM
        for (i in 0 until size) {
            if (keysAddresses[i] == Loggable.NULL_ADDRESS) {
                keysAddresses[i] = keys[i]!!.save(tree)
                result = ReclaimFlag.PRESERVE
            }
        }
        return result
    }

    override fun getByteIterables(flag: ReclaimFlag): Array<ByteIterable> {
        return arrayOf(
            getIterable(((size shl 1) + flag.value).toLong()),  // store flag bit
            CompressedUnsignedLongArrayByteIterable.getIterable(keysAddresses, size)
        )
    }

    override fun toString(): String {
        return "Bottom* [$size]"
    }

    override fun dump(out: PrintStream, level: Int, renderer: Dumpable.ToString?) {
        BottomPage.dump(out, level, renderer, this)
    }

    override fun delete(key: ByteIterable, value: ByteIterable?): Boolean {
        val pos = binarySearch(key)
        if (pos < 0) return false
        val tree = this.tree as BTreeMutable
        if (tree.allowsDuplicates) {
            val ln: ILeafNode = getKey(pos)
            if (value == null) { // size will be decreased dramatically, all dup sub-tree will expire
                if (!ln.isMutable) {
                    tree.addExpiredLoggable(ln)
                    val it = ln.addressIterator()
                    while (it.hasNext()) tree.addExpiredLoggable(it.next())
                }
                copyChildren(pos + 1, pos)
                tree.decrementSize(ln.dupCount)
                decrementSize(1)
                return true
            }
            if (ln.isDup) {
                val lnm: LeafNodeDupMutable
                var res: Boolean
                if (ln.isMutable) {
                    lnm = ln as LeafNodeDupMutable
                    res = lnm.delete(value)
                } else {
                    val lnd = ln as LeafNodeDup
                    val dupTree = lnd.treeCopyMutable
                    dupTree.mainTree = tree
                    if (dupTree.delete(value).also { res = it }) {
                        tree.addExpiredLoggable(ln.address)
                        lnm = LeafNodeDupMutable.convert(ln, tree, dupTree)
                        // remember in page
                        set(pos, lnm, null)
                    } else {
                        return false
                    }
                }
                if (res) {
                    // if only one node left
                    if (lnm.rootPage.isBottom && lnm.rootPage.size == 1) {
                        //expire previous address
                        tree.addExpiredLoggable(keysAddresses[pos])
                        //expire single duplicate from sub-tree
                        tree.addExpiredLoggable(ln.addressIterator().next())
                        // convert back to leaf without duplicates
                        set(pos, tree.createMutableLeaf(lnm.key, lnm.value), null)
                    }
                    return true
                }
                return false
            }
        }
        tree.addExpiredLoggable(keysAddresses[pos])
        copyChildren(pos + 1, pos)
        tree.decrementSize(1)
        decrementSize(1)
        return true
    }

    override fun mergeWithChildren(): BasePageMutable {
        return this
    }

    override fun mergeWithRight(page: BasePageMutable) {
        val newPageSize = size + page.size
        if (newPageSize >= keys.size) {
            val newArraySize = (newPageSize and 0x7ffffffe) + 2
            keys = keys.copyOf(newArraySize)
            keysAddresses = keysAddresses.copyOf(newArraySize)
        }
        System.arraycopy(page.keys, 0, keys, size, page.size)
        System.arraycopy(page.keysAddresses, 0, keysAddresses, size, page.size)
        size = newPageSize
    }

    override fun mergeWithLeft(page: BasePageMutable) {
        page.mergeWithRight(this)
        keys = page.keys
        keysAddresses = page.keysAddresses
        size = page.size
    }

    override fun childExists(key: ByteIterable, pageAddress: Long): Boolean {
        return false
    }

    override val type: Byte
        get() = (tree as BTreeMutable).bottomPageType

    override fun setMutableChild(index: Int, child: BasePageMutable) {
        throw UnsupportedOperationException()
    }
}