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
import jetbrains.exodus.CompoundByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.bindings.CompressedUnsignedLongArrayByteIterable;
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.log.TooBigLoggableException;
import jetbrains.exodus.tree.MutableTreeRoot;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

abstract class BasePageMutable extends BasePage implements MutableTreeRoot {

    protected BaseLeafNodeMutable[] keys;
    protected long[] keysAddresses;

    protected BasePageMutable(BTreeMutable tree) {
        super(tree);
    }

    protected BasePageMutable(BTreeMutable tree, BasePageImmutable page) {
        super(tree);
        size = page.size;
        createChildren(Math.max(page.size, getBalancePolicy().getPageMaxSize()));
        if (size > 0) {
            load(page.getDataIterator(0), page.keyAddressLen);
        }
    }

    protected void load(final ByteIterator it, final int keyAddressLen) {
        // create array with max size for key addresses
        CompressedUnsignedLongArrayByteIterable.loadLongs(keysAddresses, it, size, keyAddressLen);
    }

    @Override
    @SuppressWarnings({"ReturnOfThis"})
    @NotNull
    protected BasePageMutable getMutableCopy(BTreeMutable treeMutable) {
        return this;
    }

    @Override
    protected long getDataAddress() {
        return Loggable.NULL_ADDRESS;
    }

    protected void createChildren(int max) {
        keys = new BaseLeafNodeMutable[max];
        keysAddresses = new long[max];
    }

    /**
     * Deletes key/value pair. If key corresponds to several duplicates, remove all of them.
     *
     * @param key   key to delete
     * @param value value to delete
     * @return true iff succeeded
     */
    protected abstract boolean delete(@NotNull ByteIterable key, @Nullable ByteIterable value);

    /**
     * Insert or update value in tree.
     * If tree supports duplicates and key exists, inserts after existing key
     *
     * @param key       key to put
     * @param value     value to put
     * @param overwrite true if existing value by the key should be overwritten
     * @param result    false if key exists, overwite is false and tree is not support duplicates
     */
    protected abstract BasePageMutable put(@NotNull ByteIterable key, @NotNull ByteIterable value, boolean overwrite, boolean[] result);

    public abstract BasePageMutable putRight(@NotNull ByteIterable key, @NotNull ByteIterable value);

    /**
     * Serialize page data
     *
     * @return serialized data
     */
    protected ByteIterable getData() {
        return new CompoundByteIterable(getByteIterables(saveChildren()));
    }

    @NotNull
    protected abstract ReclaimFlag saveChildren();

    protected abstract ByteIterable[] getByteIterables(ReclaimFlag flag);

    /**
     * Save page to log
     *
     * @return address of this page after save
     */
    protected long save() {
        // save leaf nodes
        ReclaimFlag flag = saveChildren();
        // save self. complementary to {@link load()}
        final byte type = getType();
        final BTreeBase tree = getTree();
        final int structureId = tree.structureId;
        final Log log = tree.log;
        if (flag == ReclaimFlag.PRESERVE) {
            // there is a chance to update the flag to RECLAIM
            if (log.getHighAddress() % log.getFileSize() == 0) {
                // page will be exactly on file border
                flag = ReclaimFlag.RECLAIM;
            } else {
                final ByteIterable[] iterables = getByteIterables(flag);
                long result = log.tryWrite(type, structureId, new CompoundByteIterable(iterables));
                if (result < 0) {
                    iterables[0] = CompressedUnsignedLongByteIterable.getIterable(
                        (size << 1) + ReclaimFlag.RECLAIM.value
                    );
                    result = log.writeContinuously(type, structureId, new CompoundByteIterable(iterables));

                    if (result < 0) {
                        throw new TooBigLoggableException();
                    }
                }
                return result;
            }
        }
        return log.write(type, structureId, new CompoundByteIterable(getByteIterables(flag)));
    }

    protected abstract byte getType();

    @Override
    protected long getKeyAddress(int index) {
        return keysAddresses[index];
    }

    @Override
    @NotNull
    public BaseLeafNode getKey(int index) {
        if (index >= size) {
            throw new ArrayIndexOutOfBoundsException(index + " >= " + size);
        }
        return keys[index] == null ? getTree().loadLeaf(keysAddresses[index]) : keys[index];
    }

    protected abstract void setMutableChild(int index, @NotNull BasePageMutable child);

    protected BTreeBalancePolicy getBalancePolicy() {
        return getTree().getBalancePolicy();
    }

    @Override
    protected boolean isMutable() {
        return true;
    }

    @Nullable
    protected BasePageMutable insertAt(int pos, @NotNull ILeafNode key, @Nullable BasePageMutable child) {
        if (!getBalancePolicy().needSplit(this)) {
            insertDirectly(pos, key, child);
            return null;
        } else {
            int splitPos = getBalancePolicy().getSplitPos(this, pos);

            final BasePageMutable sibling = split(splitPos, size - splitPos);
            if (pos >= splitPos) {
                // insert into right sibling
                sibling.insertAt(pos - splitPos, key, child);
            } else {
                // insert into self
                insertAt(pos, key, child);
            }
            return sibling;
        }
    }

    protected void set(int pos, @NotNull ILeafNode key, @Nullable BasePageMutable child) {
        // do not remember immutable leaf, but only address
        if (key instanceof BaseLeafNodeMutable) {
            keys[pos] = (BaseLeafNodeMutable) key;
        } else {
            keys[pos] = null; // forget previous mutable leaf
        }
        keysAddresses[pos] = key.getAddress();
    }

    protected void insertDirectly(final int pos, @NotNull ILeafNode key, @Nullable BasePageMutable child) {
        if (pos < size) {
            copyChildren(pos, pos + 1);
        }
        size += 1;
        set(pos, key, child);
    }

    protected void copyChildren(final int from, final int to) {
        if (from >= size) return;
        System.arraycopy(keys, from, keys, to, size - from);
        System.arraycopy(keysAddresses, from, keysAddresses, to, size - from);
    }

    @Override
    protected int binarySearch(final ByteIterable key) {
        return binarySearch(key, 0);
    }

    @Override
    protected int binarySearch(final ByteIterable key, final int low) {
        return binarySearch(this, key, low, getSize() - 1);
    }

    protected void decrementSize(final int value) {
        if (size < value) {
            throw new ExodusException("Can't decrease BTree page size " + size + " on " + value);
        }
        final int initialSize = size;
        size -= value;
        for (int i = size; i < initialSize; ++i) {
            keys[i] = null;
            keysAddresses[i] = 0L;
        }
    }

    protected abstract BasePageMutable split(int from, int length);

    protected abstract BasePageMutable mergeWithChildren();

    protected abstract void mergeWithRight(BasePageMutable page);

    protected abstract void mergeWithLeft(BasePageMutable page);

    protected static int binarySearch(final @NotNull BasePage page,
                                      final @NotNull ByteIterable key,
                                      int low, int high) {
        while (low <= high) {
            final int mid = (low + high + 1) >>> 1;
            final ILeafNode midKey = page.getKey(mid);
            int cmp = midKey.compareKeyTo(key);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                // key found
                return mid;
            }
        }
        // key not found
        return -(low + 1);
    }

    protected enum ReclaimFlag {
        PRESERVE(0),
        RECLAIM(1);

        final int value;

        ReclaimFlag(int value) {
            this.value = value;
        }
    }
}
