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
import jetbrains.exodus.bindings.CompressedUnsignedLongArrayByteIterable;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.tree.LongIterator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;

/**
 */
class BottomPageMutable extends BasePageMutable {

    BottomPageMutable(BTreeMutable tree, BottomPage page) {
        super(tree, page);
    }

    private BottomPageMutable(BottomPageMutable page, int from, int length) {
        super((BTreeMutable) page.getTree());

        final int max = Math.max(length, getBalancePolicy().getPageMaxSize());
        keys = new BaseLeafNodeMutable[max];
        keysAddresses = new long[max];

        System.arraycopy(page.keys, from, keys, 0, length);
        System.arraycopy(page.keysAddresses, from, keysAddresses, 0, length);

        size = length;
    }

    @Override
    protected boolean isBottom() {
        return true;
    }

    @Override
    public long getChildAddress(int index) {
        return keysAddresses[index];
    }

    @Override
    @Nullable
    public BasePageMutable put(@NotNull ByteIterable key, @NotNull ByteIterable value, boolean overwrite, boolean[] result) {
        final BTreeMutable tree = (BTreeMutable) getTree();

        int pos = binarySearch(key);
        if (pos >= 0) {
            if (overwrite) {
                // key found
                final ILeafNode ln = getKey(pos);
                if (tree.allowsDuplicates) {
                    // overwrite for tree with duplicates means add new value to existing key
                    // manage sub-tree of duplicates
                    // ln may be mutable or immutable, with dups or without
                    LeafNodeDupMutable lnm = LeafNodeDupMutable.convert(ln, tree);
                    if (lnm.put(value)) {
                        tree.addExpiredLoggable(ln.getAddress());
                        set(pos, lnm, null);
                        result[0] = true;
                    }
                    // main tree size will be auto-incremented with some help from duplicates tree
                } else {
                    if (!ln.isDupLeaf()) {
                        // TODO: remove this forced update when we no longer need meta tree cloning
                        tree.addExpiredLoggable(keysAddresses[pos]);
                        set(pos, tree.createMutableLeaf(key, value), null);
                        // this should be always true in order to keep up with keysAddresses[pos] expiration
                        result[0] = true;
                    }
                }
            }
            return null;
        }

        // if found - insert at this position, else insert after found
        if (pos < 0) pos = -pos - 1;
        else pos++;

        final BasePageMutable page = insertAt(pos, tree.createMutableLeaf(key, value), null);
        result[0] = true;
        tree.incrementSize();
        return page;
    }

    @Override
    @Nullable
    public BasePageMutable putRight(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        final BTreeMutable tree = (BTreeMutable) getTree();
        if (size > 0) {
            final int pos = size - 1;
            final BaseLeafNode ln = getKey(pos);
            final int cmp = ln.compareKeyTo(key);
            if (cmp > 0) {
                throw new IllegalArgumentException("Key must be greater");
            } else if (cmp == 0) {
                if (tree.allowsDuplicates) {
                    set(pos, LeafNodeDupMutable.convert(ln, tree).putRight(value), null);
                    tree.addExpiredLoggable(ln.getAddress());
                    return null;
                } else {
                    throw new IllegalArgumentException("Key must not be equal");
                }
            }
        }
        final BasePageMutable page = insertAt(size, tree.createMutableLeaf(key, value), null);
        tree.incrementSize();
        return page;
    }

    @Override
    protected BasePageMutable split(int from, int length) {
        final BottomPageMutable result = new BottomPageMutable(this, from, length);
        decrementSize(length);
        return result;
    }

    @Override
    protected long getBottomPagesCount() {
        return 1;
    }

    @Override
    public ILeafNode get(@NotNull ByteIterable key) {
        return BottomPage.get(key, this);
    }

    @Override
    public ILeafNode find(@NotNull BTreeTraverser stack, int depth, @NotNull ByteIterable key, @Nullable ByteIterable value, boolean equalOrNext) {
        return BottomPage.find(stack, depth, key, value, equalOrNext, this);
    }

    @Override
    public boolean keyExists(@NotNull ByteIterable key) {
        return BottomPage.keyExists(key, this);
    }

    @Override
    public boolean exists(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        return BottomPage.exists(key, value, this);
    }

    @NotNull
    @Override
    protected ReclaimFlag saveChildren() {
        final BTreeBase tree = getTree();
        ReclaimFlag result = ReclaimFlag.RECLAIM;
        for (int i = 0; i < size; i++) {
            if (keysAddresses[i] == Loggable.NULL_ADDRESS) {
                keysAddresses[i] = keys[i].save(tree);
                result = ReclaimFlag.PRESERVE;
            }
        }
        return result;
    }

    @Override
    protected ByteIterable[] getByteIterables(@NotNull final ReclaimFlag flag) {
        return new ByteIterable[]{
                CompressedUnsignedLongByteIterable.getIterable((size << 1) + flag.value), // store flag bit
                CompressedUnsignedLongArrayByteIterable.getIterable(keysAddresses, size)
        };
    }

    @Override
    public String toString() {
        return "Bottom* [" + size + ']';
    }

    @Override
    public void dump(PrintStream out, int level, ToString renderer) {
        BottomPage.dump(out, level, renderer, this);
    }

    @Override
    public boolean delete(@NotNull ByteIterable key, @Nullable ByteIterable value) {
        final int pos = binarySearch(key);
        if (pos < 0) return false;

        final BTreeMutable tree = (BTreeMutable) getTree();

        if (tree.allowsDuplicates) {
            final ILeafNode ln = getKey(pos);
            if (value == null) { // size will be decreased dramatically, all dup sub-tree will expire
                tree.addExpiredLoggable(keysAddresses[pos]);
                LongIterator it = ln.addressIterator();
                while (it.hasNext()) tree.addExpiredLoggable(it.next());
                copyChildren(pos + 1, pos);
                tree.decrementSize(ln.getDupCount());
                decrementSize(1);
                return true;
            }
            if (ln.isDup()) {
                LeafNodeDupMutable lnm;
                boolean res;
                if (ln.isMutable()) {
                    lnm = (LeafNodeDupMutable) ln;
                    res = lnm.delete(value);
                } else {
                    LeafNodeDup lnd = (LeafNodeDup) ln;

                    final BTreeDupMutable dupTree = lnd.getTreeCopyMutable();
                    dupTree.mainTree = tree;

                    if (res = dupTree.delete(value)) {
                        tree.addExpiredLoggable(ln.getAddress());
                        lnm = LeafNodeDupMutable.convert(ln, tree, dupTree);
                        // remember in page
                        set(pos, lnm, null);
                    } else {
                        return false;
                    }
                }

                if (res) {
                    // if only one node left
                    if (lnm.getRootPage().isBottom() && lnm.getRootPage().getSize() == 1) {
                        //expire previous address
                        tree.addExpiredLoggable(keysAddresses[pos]);
                        //expire single duplicate from sub-tree
                        LongIterator it = ln.addressIterator();
                        tree.addExpiredLoggable(it.next());
                        // convert back to leaf without duplicates
                        set(pos, tree.createMutableLeaf(lnm.getKey(), lnm.getValue()), null);
                    }
                    return true;
                }
                return false;
            }
        }

        tree.addExpiredLoggable(keysAddresses[pos]);
        copyChildren(pos + 1, pos);
        tree.decrementSize(1);
        decrementSize(1);

        return true;
    }

    @Override
    protected BasePageMutable mergeWithChildren() {
        return this;
    }

    @Override
    protected void mergeWithRight(BasePageMutable page) {
        System.arraycopy(page.keys, 0, keys, size, page.size);
        System.arraycopy(page.keysAddresses, 0, keysAddresses, size, page.size);
        size += page.size;
    }

    @Override
    protected void mergeWithLeft(BasePageMutable page) {
        page.mergeWithRight(this);
        keys = page.keys;
        keysAddresses = page.keysAddresses;
        size = page.size;
    }

    @Override
    public boolean childExists(@NotNull ByteIterable key, long pageAddress) {
        return false;
    }

    @Override
    protected byte getType() {
        return ((BTreeMutable) getTree()).getBottomPageType();
    }

    @Override
    protected void setMutableChild(int index, @NotNull BasePageMutable child) {
        throw new UnsupportedOperationException();
    }
}