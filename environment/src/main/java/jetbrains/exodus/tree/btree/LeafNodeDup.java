/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.tree.btree;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.log.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;

/**
 * Leaf node with root page of duplicates sub-tree as a value
 */
class LeafNodeDup extends LeafNode {

    @NotNull
    protected final BTreeDup tree;

    LeafNodeDup(@NotNull BTreeBase mainTree, @NotNull RandomAccessLoggable loggable) {
        super(loggable);
        tree = new BTreeDup(mainTree, this);
    }

    @NotNull
    @Override
    public ByteIterable getValue() {
        return tree.getRoot().getMinKey().getKey();
    }

    @Override
    public int compareValueTo(@NotNull ByteIterable iterable) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public BTreeBase getTree() {
        return tree;
    }

    @NotNull
    @Override
    public AddressIterator addressIterator() {
        final BTreeTraverser traverser = BTreeMutatingTraverser.create(tree);
        return new AddressIterator(null, traverser.currentNode.size > 0, traverser);
    }

    @Override
    public boolean hasValue() {
        return false;
    }

    @Override
    public boolean isDup() {
        return true;
    }

    @Override
    public long getDupCount() {
        return tree.size;
    }

    @Override
    public boolean valueExists(@NotNull ByteIterable value) {
        // value is a key in duplicates sub-tree
        return tree.hasKey(value);
    }

    @NotNull
    BTreeDupMutable getTreeCopyMutable() {
        return tree.getMutableCopy();
    }

    @Override
    public String toString() {
        return "LND {key:" + getKey().toString() + '}';
    }

    @Override
    public void dump(PrintStream out, int level, ToString render) {
        super.dump(out, level, render);
        tree.getRoot().dump(out, level + 1, render);
    }

    @Override
    public void doReclaim(@NotNull final BTreeReclaimTraverser context, final int leafIndex) {
        final long keyAddress = context.currentNode.getKeyAddress(leafIndex);
        final BTreeDupMutable tree;
        final BaseLeafNodeMutable mutable;
        final BTreeMutable mainTree = context.mainTree;
        if (keyAddress < 0) {
            mutable = ((BasePageMutable) context.currentNode).keys[leafIndex];
            if (mutable.isDup()) {
                tree = ((LeafNodeDupMutable) mutable).tree;
            } else {
                return; // current node is duplicate no more
            }
        } else if (keyAddress == getAddress()) {
            final BasePageMutable node = context.currentNode.getMutableCopy(mainTree);
            final LeafNodeDupMutable converted = LeafNodeDupMutable.convert(this, mainTree);
            mainTree.addExpiredLoggable(keyAddress);
            tree = converted.tree;
            mutable = converted;
            node.set(leafIndex, mutable, null);
            context.wasReclaim = true;
            context.setPage(node);
        } else {
            final RandomAccessLoggable upToDate = mainTree.getLoggable(keyAddress);
            switch (upToDate.getType()) {
                case BTreeBase.LEAF_DUP_BOTTOM_ROOT:
                case BTreeBase.LEAF_DUP_INTERNAL_ROOT:
                    mutable = null; // indicates that loggable was not updated
                    tree = new LeafNodeDup(mainTree, upToDate).getTreeCopyMutable();
                    tree.mainTree = mainTree;
                    break;
                case BTreeBase.LEAF:
                    return; // current node is duplicate no more
                default:
                    throw new ExodusException("Unexpected loggable type " + upToDate.getType());
            }
        }
        // TODO: implement mutable lower bound to avoid allocations (yapavel knows how)
        final BTreeReclaimTraverser dupStack = new BTreeReclaimTraverser(tree);
        for (final RandomAccessLoggable loggable : context.dupLeafsLo) {
            switch (loggable.getType()) {
                case BTreeBase.DUP_LEAF:
                    new LeafNode(loggable).reclaim(dupStack);
                    break;
                case BTreeBase.DUP_BOTTOM:
                    tree.reclaimBottom(loggable, dupStack);
                    break;
                case BTreeBase.DUP_INTERNAL:
                    tree.reclaimInternal(loggable, dupStack);
                    break;
                default:
                    throw new ExodusException("Unexpected loggable type " + loggable.getType());
            }
        }
        // TODO: less copy-paste
        for (final RandomAccessLoggable loggable : context.dupLeafsHi) {
            switch (loggable.getType()) {
                case BTreeBase.DUP_LEAF:
                    new LeafNode(loggable).reclaim(dupStack);
                    break;
                case BTreeBase.DUP_BOTTOM:
                    tree.reclaimBottom(loggable, dupStack);
                    break;
                case BTreeBase.DUP_INTERNAL:
                    tree.reclaimInternal(loggable, dupStack);
                    break;
                default:
                    throw new ExodusException("Unexpected loggable type " + loggable.getType());
            }
        }
        while (dupStack.canMoveUp()) {
            // wire up mutated stuff
            dupStack.popAndMutate();
        }
        if (dupStack.wasReclaim && mutable == null) { // was reclaim in sub-tree
            mainTree.addExpiredLoggable(keyAddress);
            final BasePageMutable node = context.currentNode.getMutableCopy(mainTree);
            node.set(leafIndex, new LeafNodeDupMutable(tree), null);
            context.wasReclaim = true;
            context.setPage(node);
        }
        //TODO: remove? dupStack.clear();
    }

    @Override
    protected void reclaim(@NotNull final BTreeReclaimTraverser context) {
        final long startAddress = tree.getStartAddress();
        final Log log = tree.log;
        if (startAddress != Loggable.NULL_ADDRESS && log.hasAddress(startAddress)) {
            final BoundLoggableIterator itr = new BoundLoggableIterator(log.getLoggableIterator(startAddress), log.getFileAddress(getAddress()));
            collect(context.dupLeafsLo, itr.next(), itr);
        }
        super.reclaim(context);
    }

    @Nullable
    static RandomAccessLoggable collect(@NotNull final List<RandomAccessLoggable> output,
                                        @NotNull RandomAccessLoggable loggable,
                                        @NotNull final Iterator<RandomAccessLoggable> loggables) {
        while (true) {
            switch (loggable.getType()) {
                case NullLoggable.TYPE:
                    break;
                case BTreeBase.LEAF_DUP_BOTTOM_ROOT:
                case BTreeBase.LEAF_DUP_INTERNAL_ROOT:
                    return loggable; // enough duplicates, just yield
                case BTreeBase.DUP_LEAF:
                case BTreeBase.DUP_BOTTOM:
                case BTreeBase.DUP_INTERNAL:
                    output.add(loggable);
                    break;
                default:
                    throw new ExodusException("Unexpected loggable type " + loggable.getType());
            }
            if (loggables.hasNext()) {
                loggable = loggables.next();
            } else {
                break;
            }
        }
        return null;
    }

    private static final class BoundLoggableIterator implements Iterator<RandomAccessLoggable> {
        @NotNull
        private final LoggableIterator data;
        private final long upperBound;

        private BoundLoggableIterator(@NotNull final LoggableIterator data, final long upperBound) {
            this.data = data;
            this.upperBound = upperBound;
        }

        @Override
        public boolean hasNext() {
            return data.hasNext() && data.getHighAddress() < upperBound;
        }

        @Override
        public RandomAccessLoggable next() {
            return data.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
