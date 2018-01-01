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
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.bindings.CompressedUnsignedLongArrayByteIterable;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.log.Loggable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.PrintStream;

/**
 */
public class InternalPageMutable extends BasePageMutable {
    protected BasePageMutable[] children;
    protected long[] childrenAddresses;

    InternalPageMutable(BTreeMutable tree, InternalPage page) {
        super(tree, page);
    }

    private InternalPageMutable(InternalPageMutable page, int from, int length) {
        super((BTreeMutable) page.getTree());

        createChildren(Math.max(length, getBalancePolicy().getPageMaxSize()));

        System.arraycopy(page.keys, from, keys, 0, length);
        System.arraycopy(page.keysAddresses, from, keysAddresses, 0, length);
        System.arraycopy(page.children, from, children, 0, length);
        System.arraycopy(page.childrenAddresses, from, childrenAddresses, 0, length);

        size = length;
    }

    InternalPageMutable(BTreeMutable tree, BasePageMutable page1, BasePageMutable page2) {
        super(tree);

        createChildren(getBalancePolicy().getPageMaxSize());
        set(0, page1.getMinKey(), page1);
        set(1, page2.getMinKey(), page2);
        size = 2;
    }

    @Override
    protected void load(@NotNull final ByteIterator it, final int keyAddressLen) {
        super.load(it, keyAddressLen);
        CompressedUnsignedLongArrayByteIterable.loadLongs(childrenAddresses, it, size);
    }

    @Override
    protected boolean isBottom() {
        return false;
    }

    @Override
    protected void createChildren(int max) {
        super.createChildren(max);
        children = new BasePageMutable[max];
        childrenAddresses = new long[max];
    }

    @Override
    public long getChildAddress(int index) {
        return childrenAddresses[index];
    }

    @Override
    @NotNull
    public BasePage getChild(int index) {
        if (children[index] == null) {
            return getTree().loadPage(childrenAddresses[index]);
        }

        return children[index];
    }

    @Override
    public boolean childExists(@NotNull ByteIterable key, long pageAddress) {
        final int index = InternalPage.binarySearchGuessUnsafe(this, key);
        return index >= 0 && (childrenAddresses[index] == pageAddress || getChild(index).childExists(key, pageAddress));
    }

    @Override
    protected byte getType() {
        return ((BTreeMutable) getTree()).getInternalPageType();
    }

    @NotNull
    private BasePageMutable getMutableChild(int index) {
        if (index >= size) {
            throw new ArrayIndexOutOfBoundsException(index + " >= " + size);
        }

        final BTreeMutable tree = (BTreeMutable) getTree();

        if (children[index] == null) {
            final long childAddress = childrenAddresses[index];
            tree.addExpiredLoggable(childAddress);
            children[index] = tree.loadPage(childAddress).getMutableCopy(tree);
            // loaded mutable page will be changed and must be saved
            childrenAddresses[index] = Loggable.NULL_ADDRESS;
        }
        return children[index];
    }

    @Override
    protected void setMutableChild(int index, @NotNull BasePageMutable child) {
        final BaseLeafNodeMutable key = child.keys[0];
        if (key != null) { // first key is mutable ==> changed, no merges or reclaims allowed
            keys[index] = key;
            keysAddresses[index] = key.getAddress();
        }
        children[index] = child;
        ((BTreeMutable) getTree()).addExpiredLoggable(childrenAddresses[index]);
        childrenAddresses[index] = Loggable.NULL_ADDRESS;
    }

    @Override
    @Nullable
    public BasePageMutable put(@NotNull ByteIterable key, @NotNull ByteIterable value, boolean overwrite, boolean[] result) {
        int pos = binarySearch(key);

        if (pos >= 0 && !overwrite) {
            // key found and overwrite is not possible - error
            return null;
        }

        if (pos < 0) {
            pos = -pos - 2;
            // if insert after last - set to last
            if (pos < 0) pos = 0;
        }

        final BTreeMutable tree = (BTreeMutable) getTree();
        final BasePageMutable child = getChild(pos).getMutableCopy(tree);
        final BasePageMutable newChild = child.put(key, value, overwrite, result);
        // change min key for child
        if (result[0]) {
            tree.addExpiredLoggable(childrenAddresses[pos]);
            set(pos, child.getMinKey(), child);
            if (newChild != null) {
                return insertAt(pos + 1, newChild.getMinKey(), newChild);
            }
        }

        return null;
    }

    @Override
    @Nullable
    public BasePageMutable putRight(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        int pos = size - 1;
        final BTreeMutable tree = (BTreeMutable) getTree();
        final BasePageMutable child = getChild(pos).getMutableCopy(tree);
        BasePageMutable newChild = child.putRight(key, value);
        // change min key for child
        tree.addExpiredLoggable(childrenAddresses[pos]);
        set(pos, child.getMinKey(), child);
        if (newChild != null) {
            return insertAt(pos + 1, newChild.getMinKey(), newChild);
        }

        return null;
    }

    @Override
    protected void set(int pos, @NotNull ILeafNode key, @Nullable BasePageMutable child) {
        super.set(pos, key, child);
        // remember mutable page and reset address to save it later
        children[pos] = child;
        childrenAddresses[pos] = Loggable.NULL_ADDRESS;
    }

    @Override
    protected void copyChildren(int from, int to) {
        if (from >= size) return;
        super.copyChildren(from, to);
        System.arraycopy(children, from, children, to, size - from);
        System.arraycopy(childrenAddresses, from, childrenAddresses, to, size - from);
    }

    @Override
    protected void decrementSize(final int value) {
        final int initialSize = size;
        super.decrementSize(value);
        for (int i = size; i < initialSize; ++i) {
            children[i] = null;
            childrenAddresses[i] = 0L;
        }
    }

    @Override
    protected BasePageMutable split(int from, int length) {
        final InternalPageMutable result = new InternalPageMutable(this, from, length);
        decrementSize(length);
        return result;
    }

    @Override
    public ILeafNode get(@NotNull final ByteIterable key) {
        return InternalPage.get(key, this);
    }

    @Override
    public ILeafNode find(@NotNull BTreeTraverser stack, int depth, @NotNull ByteIterable key, @Nullable ByteIterable value, boolean equalOrNext) {
        return InternalPage.find(stack, depth, key, value, equalOrNext, this);
    }

    @Override
    public boolean keyExists(@NotNull ByteIterable key) {
        return InternalPage.keyExists(key, this);
    }

    @Override
    public boolean exists(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        return InternalPage.exists(key, value, this);
    }

    @Override
    protected long getBottomPagesCount() {
        long result = 0;
        for (int i = 0; i < getSize(); i++) {
            result += getChild(i).getBottomPagesCount();
        }
        return result;
    }

    @NotNull
    @Override
    protected ReclaimFlag saveChildren() {
        // save children to get their addresses
        ReclaimFlag result = ReclaimFlag.RECLAIM;
        for (int i = 0; i < size; i++) {
            if (childrenAddresses[i] == Loggable.NULL_ADDRESS) {
                childrenAddresses[i] = children[i].save();
                keysAddresses[i] = children[i].keysAddresses[0];
                result = ReclaimFlag.PRESERVE;
            }
        }
        return result;
    }

    @Override
    protected ByteIterable[] getByteIterables(@NotNull final ReclaimFlag flag) {
        return new ByteIterable[]{
                CompressedUnsignedLongByteIterable.getIterable((size << 1) + flag.value),
                CompressedUnsignedLongArrayByteIterable.getIterable(keysAddresses, size),
                CompressedUnsignedLongArrayByteIterable.getIterable(childrenAddresses, size),
        };
    }

    @Override
    public String toString() {
        return "Internal* [" + size + ']';
    }

    @Override
    public void dump(PrintStream out, int level, ToString renderer) {
        InternalPage.dump(out, level, renderer, this);
    }

    @Override
    protected void mergeWithRight(BasePageMutable _page) {
        InternalPageMutable page = (InternalPageMutable) _page;
        System.arraycopy(page.keys, 0, keys, size, page.size);
        System.arraycopy(page.keysAddresses, 0, keysAddresses, size, page.size);
        System.arraycopy(page.children, 0, children, size, page.size);
        System.arraycopy(page.childrenAddresses, 0, childrenAddresses, size, page.size);
        size += page.size;
    }

    @Override
    protected void mergeWithLeft(BasePageMutable _page) {
        InternalPageMutable page = (InternalPageMutable) _page;
        page.mergeWithRight(this);
        keys = page.keys;
        keysAddresses = page.keysAddresses;
        children = page.children;
        childrenAddresses = page.childrenAddresses;
        size = page.size;
    }

    protected void removeChild(int pos) {
        copyChildren(pos + 1, pos);
        decrementSize(1);
    }

    @Override
    public boolean delete(@NotNull ByteIterable key, @Nullable ByteIterable value) {
        int pos = InternalPage.binarySearchGuess(this, key);
        final BasePageMutable child = getMutableChild(pos);
        if (!child.delete(key, value)) {
            return false;
        }
        // if first element was removed in child, then update min key
        final int childSize = child.getSize();
        if (childSize > 0) {
            set(pos, child.getMinKey(), child);
        }
        final BTreeBalancePolicy balancePolicy = getBalancePolicy();
        if (pos > 0) {
            final BasePage left = getChild(pos - 1);
            if (balancePolicy.needMerge(left, child)) {
                // merge child into left sibling
                // reget mutable left
                getMutableChild(pos - 1).mergeWithRight(child);
                removeChild(pos);
            }
        } else if (pos + 1 < getSize()) {
            final BasePage right = getChild(pos + 1);
            if (balancePolicy.needMerge(child, right)) {
                // merge child with right sibling
                final BasePageMutable mutableRight = getMutableChild(pos + 1);
                mutableRight.mergeWithLeft(child);
                removeChild(pos);
                // change key for link to right
                set(pos, mutableRight.getMinKey(), mutableRight);
            }
        } else if (childSize == 0) {
            removeChild(pos);
        }
        return true;
    }

    @Override
    protected BasePageMutable mergeWithChildren() {
        BasePageMutable result = this;
        while (!result.isBottom() && result.getSize() == 1) {
            result = ((InternalPageMutable) result).getMutableChild(0);
        }
        return result;
    }

}
