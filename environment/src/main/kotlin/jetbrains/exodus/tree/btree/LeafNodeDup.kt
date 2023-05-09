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
import jetbrains.exodus.log.*
import jetbrains.exodus.tree.Dumpable
import java.io.PrintStream

/**
 * Leaf node with root page of duplicates sub-tree as a value
 */
internal class LeafNodeDup(mainTree: BTreeBase, loggable: RandomAccessLoggable) : LeafNode(mainTree.log, loggable) {
    private val tree: BTreeDup

    init {
        tree = BTreeDup(mainTree, this)
    }

    override fun getTree(): BTreeBase {
        return tree
    }

    override fun getValue(): ByteIterable = tree.getRoot().getMinKey().getKey()

    override fun compareValueTo(iterable: ByteIterable): Int {
        throw UnsupportedOperationException()
    }

    override fun addressIterator(): AddressIterator {
        val traverser: BTreeTraverser = BTreeMutatingTraverser.create(tree)
        return AddressIterator(null, traverser.currentNode.size > 0, traverser)
    }

    override fun hasValue(): Boolean {
        return false
    }

    override fun isDup(): Boolean = true
    override fun getDupCount(): Long = tree.size

    override fun valueExists(value: ByteIterable): Boolean {
        // value is a key in duplicates sub-tree
        return tree.hasKey(value)
    }

    fun getTreeCopyMutable(): BTreeDupMutable = tree.getMutableCopy()

    override fun toString(): String {
        return "LND {key:${getKey()}}"
    }

    override fun dump(out: PrintStream, level: Int, renderer: Dumpable.ToString?) {
        super.dump(out, level, renderer)
        tree.getRoot().dump(out, level + 1, renderer)
    }

    public override fun doReclaim(context: BTreeReclaimTraverser, leafIndex: Int) {
        val keyAddress = context.currentNode.getKeyAddress(leafIndex)
        val tree: BTreeDupMutable
        val mutable: BaseLeafNodeMutable?
        val mainTree = context.mainTree
        if (keyAddress < 0) {
            mutable = (context.currentNode as BasePageMutable).keys!![leafIndex]
            tree = if (mutable!!.isDup()) {
                (mutable as LeafNodeDupMutable?)!!.tree
            } else {
                return  // current node is duplicate no more
            }
        } else if (keyAddress == getAddress()) {
            val node = context.currentNode.getMutableCopy(mainTree)
            val converted: LeafNodeDupMutable = LeafNodeDupMutable.convert(this, mainTree)
            mainTree.addExpiredLoggable(keyAddress)
            tree = converted.tree
            mutable = converted
            node[leafIndex, mutable] = null
            context.wasReclaim = true
            context.setPage(node)
        } else {
            val upToDate = mainTree.getLoggable(keyAddress)
            when (upToDate.getType()) {
                BTreeBase.LEAF_DUP_BOTTOM_ROOT, BTreeBase.LEAF_DUP_INTERNAL_ROOT -> {
                    mutable = null // indicates that loggable was not updated
                    tree = LeafNodeDup(mainTree, upToDate).getTreeCopyMutable()
                    tree.mainTree = mainTree
                }

                BTreeBase.LEAF -> return  // current node is duplicate no more
                else -> throw ExodusException("Unexpected loggable type " + upToDate.getType())
            }
        }

        val dupStack = BTreeReclaimTraverser(tree)
        for (loggable in context.dupLeafsLo) {
            when (loggable!!.getType()) {
                BTreeBase.DUP_LEAF -> LeafNode(log, loggable).reclaim(dupStack)
                BTreeBase.DUP_BOTTOM -> tree.reclaimBottom(loggable, dupStack)
                BTreeBase.DUP_INTERNAL -> tree.reclaimInternal(loggable, dupStack)
                else -> throw ExodusException("Unexpected loggable type " + loggable.getType())
            }
        }

        for (loggable in context.dupLeafsHi) {
            when (loggable!!.getType()) {
                BTreeBase.DUP_LEAF -> LeafNode(log, loggable).reclaim(dupStack)
                BTreeBase.DUP_BOTTOM -> tree.reclaimBottom(loggable, dupStack)
                BTreeBase.DUP_INTERNAL -> tree.reclaimInternal(loggable, dupStack)
                else -> throw ExodusException("Unexpected loggable type " + loggable.getType())
            }
        }
        while (dupStack.canMoveUp()) {
            // wire up mutated stuff
            dupStack.popAndMutate()
        }
        if (dupStack.wasReclaim && mutable == null) { // was reclaim in sub-tree
            mainTree.addExpiredLoggable(keyAddress)
            val node = context.currentNode.getMutableCopy(mainTree)
            node[leafIndex, LeafNodeDupMutable(tree)] = null
            context.wasReclaim = true
            context.setPage(node)
        }
    }

    override fun reclaim(context: BTreeReclaimTraverser) {
        val startAddress = tree.startAddress
        val log = tree.log
        if (startAddress != Loggable.NULL_ADDRESS && log.hasAddress(startAddress)) {
            val itr = BoundLoggableIterator(log.getLoggableIterator(startAddress), log.getFileAddress(getAddress()))
            collect(context.dupLeafsLo, itr.next(), itr)
        }
        super.reclaim(context)
    }

    private class BoundLoggableIterator(private val data: LoggableIterator, private val upperBound: Long) :
        MutableIterator<RandomAccessLoggable> {
        override fun hasNext(): Boolean {
            return data.hasNext() && data.getHighAddress() < upperBound
        }

        override fun next(): RandomAccessLoggable {
            return data.next()
        }

        override fun remove() {
            throw UnsupportedOperationException()
        }
    }

    companion object {
        fun collect(
            output: MutableList<RandomAccessLoggable?>,
            loggable: RandomAccessLoggable,
            loggables: Iterator<RandomAccessLoggable>
        ): RandomAccessLoggable? {
            var inputLoggable = loggable
            while (true) {
                when (inputLoggable.getType()) {
                    NullLoggable.TYPE, HashCodeLoggable.TYPE -> {}
                    BTreeBase.LEAF_DUP_BOTTOM_ROOT, BTreeBase.LEAF_DUP_INTERNAL_ROOT -> return inputLoggable // enough duplicates, just yield
                    BTreeBase.DUP_LEAF, BTreeBase.DUP_BOTTOM, BTreeBase.DUP_INTERNAL -> output.add(
                        inputLoggable
                    )

                    else -> throw ExodusException("Unexpected loggable type " + inputLoggable.getType())
                }
                inputLoggable = if (loggables.hasNext()) {
                    loggables.next()
                } else {
                    break
                }
            }
            return null
        }
    }
}
