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
import jetbrains.exodus.tree.Dumpable
import jetbrains.exodus.tree.ITree
import java.io.PrintStream

/**
 * Stateful leaf node with root page of duplicates sub-tree as a value
 */
internal class LeafNodeDupMutable(@JvmField val tree: BTreeDupMutable) : BaseLeafNodeMutable() {
    override fun getAddress(): Long = tree.address

    override fun getTree(): BTreeBase = tree

    override fun addressIterator(): AddressIterator {
        val traverser: BTreeTraverser = BTreeMutatingTraverser.create(tree)
        return AddressIterator(null, traverser.currentNode.size > 0, traverser)
    }

    override fun hasValue(): Boolean {
        return false
    }

    override fun isDup(): Boolean = true
    override fun getDupCount(): Long = tree.size
    fun getRootPage(): BasePageMutable = tree.getRoot()

    override fun valueExists(value: ByteIterable): Boolean {
        // value is a key in duplicates sub-tree
        return tree.hasKey(value)
    }

    override fun compareKeyTo(iterable: ByteIterable): Int {
        return tree.key.compareTo(iterable)
    }

    override fun compareValueTo(iterable: ByteIterable): Int {
        return getValue().compareTo(iterable)
    }

    override fun getKey(): ByteIterable = tree.key
    override fun getValue(): ByteIterable = tree.root.getMinKey().getKey()

    override fun delete(value: ByteIterable?): Boolean {
        return tree.delete(value!!)
    }

    fun put(value: ByteIterable): Boolean {
        return tree.put(value, ByteIterable.EMPTY)
    }

    fun putRight(value: ByteIterable): LeafNodeDupMutable {
        tree.putRight(value, ArrayByteIterable.EMPTY)
        return this
    }

    override fun save(tree: ITree): Long {
        require(this.tree.mainTree === tree) {
            "Can't save LeafNodeDupMutable against mutable tree " +
                    "different from passed on creation"
        }
        return this.tree.save()
    }

    override fun toString(): String {
        return "LND* {key:${getKey()}}"
    }

    override fun dump(out: PrintStream, level: Int, renderer: Dumpable.ToString?) {
        super.dump(out, level, renderer)
        tree.root.dump(out, level + 1, renderer)
    }

    companion object {
        /**
         * Convert any leaf to mutable leaf with duplicates support
         *
         * @param ln       leaf node to convert
         * @param mainTree its tree
         * @return mutable copy of ln
         */
        fun convert(ln: ILeafNode, mainTree: BTreeMutable): LeafNodeDupMutable {
            val isLeafNodeDup = ln.isDup()
            if (isLeafNodeDup && ln is LeafNodeDupMutable) {
                return ln
            }

            // wrapper tree that doesn't allow duplicates
            val dupTree = if (isLeafNodeDup) (ln as LeafNodeDup).getTreeCopyMutable() else BTreeDupMutable(
                BTreeEmpty(mainTree.log, mainTree.balancePolicy, false, mainTree.structureId),
                ln.getKey()
            )
            dupTree.mainTree = mainTree
            return convert(ln, mainTree, dupTree)
        }

        fun convert(ln: ILeafNode, mainTree: BTreeMutable, dupTree: BTreeDupMutable): LeafNodeDupMutable {
            val result = LeafNodeDupMutable(dupTree)
            return if (ln.isDup()) {
                result
            } else {
                // leaf node with one value -- add it
                mainTree.decrementSize(1) // hack
                result.put(ln.getValue()!!)
                result
            }
        }
    }
}
