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

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.tree.ITree;
import org.jetbrains.annotations.NotNull;

import java.io.PrintStream;

/**
 * Stateful leaf node with root page of duplicates sub-tree as a value
 */
class LeafNodeDupMutable extends BaseLeafNodeMutable {

    @NotNull
    protected final BTreeDupMutable tree;

    LeafNodeDupMutable(@NotNull final BTreeDupMutable tree) {
        this.tree = tree;
    }

    @Override
    public long getAddress() {
        return tree.address;
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

    @NotNull
    BasePageMutable getRootPage() {
        return tree.getRoot();
    }

    @Override
    public boolean valueExists(@NotNull ByteIterable value) {
        // value is a key in duplicates sub-tree
        return tree.hasKey(value);
    }

    @Override
    public int compareKeyTo(@NotNull final ByteIterable iterable) {
        return tree.key.compareTo(iterable);
    }

    @Override
    public int compareValueTo(@NotNull final ByteIterable iterable) {
        return getValue().compareTo(iterable);
    }

    @Override
    @NotNull
    public ByteIterable getKey() {
        return tree.key;
    }

    @Override
    @NotNull
    public ByteIterable getValue() {
        return tree.getRoot().getMinKey().getKey();
    }

    @Override
    public boolean delete(ByteIterable value) {
        return tree.delete(value);
    }

    boolean put(@NotNull ByteIterable value) {
        return tree.put(value, ByteIterable.EMPTY);
    }

    LeafNodeDupMutable putRight(@NotNull ByteIterable value) {
        tree.putRight(value, ArrayByteIterable.EMPTY);
        return this;
    }

    @Override
    public long save(final ITree mainTree) {
        if (tree.mainTree != mainTree) {
            throw new IllegalArgumentException("Can't save LeafNodeDupMutable against mutable tree " +
                    "different from passed on creation");
        }
        return tree.save();
    }

    @Override
    public String toString() {
        return "LND* {key:" + getKey().toString() + '}';
    }

    @Override
    public void dump(PrintStream out, int level, ToString renderer) {
        super.dump(out, level, renderer);
        tree.getRoot().dump(out, level + 1, renderer);
    }

    /**
     * Convert any leaf to mutable leaf with duplicates support
     *
     * @param ln       leaf node to convert
     * @param mainTree its tree
     * @return mutable copy of ln
     */
    static LeafNodeDupMutable convert(@NotNull ILeafNode ln, @NotNull BTreeMutable mainTree) {

        final boolean isLeafNodeDup = ln.isDup();
        if (isLeafNodeDup && ln instanceof LeafNodeDupMutable) {
            return (LeafNodeDupMutable) ln;
        }

        // wrapper tree that doesn't allow duplicates
        final BTreeDupMutable dupTree = isLeafNodeDup ?
                ((LeafNodeDup) ln).getTreeCopyMutable() :
                new BTreeDupMutable(
                        new BTreeEmpty(mainTree.getLog(), mainTree.getBalancePolicy(), false, mainTree.getStructureId()),
                        ln.getKey()
                );
        dupTree.mainTree = mainTree;
        return convert(ln, mainTree, dupTree);
    }

    static LeafNodeDupMutable convert(@NotNull ILeafNode ln, @NotNull BTreeMutable mainTree, @NotNull BTreeDupMutable dupTree) {
        final LeafNodeDupMutable result = new LeafNodeDupMutable(dupTree);
        if (ln.isDup()) {
            return result;
        } else {
            // leaf node with one value -- add it
            mainTree.decrementSize(1); // hack
            result.put(ln.getValue());
            return result;
        }
    }
}
