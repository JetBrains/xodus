/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.log.ByteIterableWithAddress;
import jetbrains.exodus.log.ByteIteratorWithAddress;
import jetbrains.exodus.log.IByteIterableComparator;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.log.iterate.CompressedUnsignedLongByteIterable;
import org.jetbrains.annotations.NotNull;

abstract class BasePageImmutable extends BasePage implements IByteIterableComparator {

    @NotNull
    protected final BTreeBase tree;
    @NotNull
    protected final ByteIterableWithAddress data;
    protected long dataAddress;
    protected int keyAddressLen;
    private ILeafNode lastComparedKey;
    private int lastLoadedKeyIndex = -1;
    private BaseLeafNode lastLoadedKey;

    /**
     * Create empty page
     *
     * @param tree tree containing the page.
     */
    protected BasePageImmutable(@NotNull final BTreeBase tree) {
        this.tree = tree;
        data = ByteIterableWithAddress.EMPTY;
        size = 0;
        dataAddress = Loggable.NULL_ADDRESS;
    }

    /**
     * Create page and load size
     *
     * @param tree tree containing the page.
     * @param data binary data to load the page from.
     */
    protected BasePageImmutable(@NotNull final BTreeBase tree, @NotNull final ByteIterableWithAddress data) {
        this.tree = tree;
        this.data = data;
        final ByteIteratorWithAddress it = data.iterator();
        size = CompressedUnsignedLongByteIterable.getInt(it) >> 1;
        init(it);
    }

    /**
     * Create page and load size
     *
     * @param tree source tree
     * @param data  source iterator
     * @param size computed size
     */
    protected BasePageImmutable(@NotNull final BTreeBase tree, @NotNull final ByteIterableWithAddress data, int size) {
        this.tree = tree;
        this.data = data;
        this.size = size;
        init(data.iterator());
    }

    private void init(@NotNull final ByteIteratorWithAddress itr) {
        if (size > 0) {
            final int next = itr.next();
            dataAddress = itr.getAddress();
            loadAddressLengths(next);
        } else {
            dataAddress = itr.getAddress();
        }
    }

    @Override
    @NotNull
    protected BTreeBase getTree() {
        return tree;
    }

    @Override
    protected long getDataAddress() {
        return dataAddress;
    }

    protected ByteIterator getDataIterator(final int offset) {
        return dataAddress == Loggable.NULL_ADDRESS ?
                ByteIterable.EMPTY_ITERATOR : data.iterator((int) (dataAddress - data.getDataAddress() + offset));
    }

    protected void loadAddressLengths(final int length) {
        checkAddressLength(keyAddressLen = length);
    }

    protected static void checkAddressLength(long addressLen) {
        if (addressLen < 0 || addressLen > 8) {
            throw new ExodusException("Invalid length of address: " + addressLen);
        }
    }

    @Override
    protected long getKeyAddress(final int index) {
        return LongBinding.entryToUnsignedLong(getDataIterator(index * keyAddressLen), keyAddressLen);
    }

    @Override
    @NotNull
    public BaseLeafNode getKey(int index) {
        if (lastLoadedKeyIndex == index) {
            return lastLoadedKey;
        }
        lastLoadedKeyIndex = index;
        return lastLoadedKey = tree.loadLeaf(getKeyAddress(index));
    }

    @Override
    protected boolean isMutable() {
        return false;
    }

    @Override
    protected SearchRes binarySearch(final ByteIterable key) {
        return binarySearch(key, 0);
    }

    @Override
    protected SearchRes binarySearch(final ByteIterable key, final int low) {
        if (dataAddress == Loggable.NULL_ADDRESS) {
            return SearchRes.NOT_FOUND;
        }
        final int index = ByteIterableWithAddress.binarySearch(
                this, key, low, size - 1, keyAddressLen, tree.log, dataAddress);
        return index >= 0 ? new SearchRes(index, lastComparedKey) : new SearchRes(index);
    }

    @Override
    public int compare(long leftAddress, ByteIterable right) {
        return (lastComparedKey = tree.loadLeaf(leftAddress)).compareKeyTo(right);
    }

    protected static void doReclaim(BTreeReclaimTraverser context) {
        final BasePageMutable node = context.currentNode.getMutableCopy(context.mainTree);
        context.wasReclaim = true;
        context.setPage(node);
        context.popAndMutate();
    }
}
