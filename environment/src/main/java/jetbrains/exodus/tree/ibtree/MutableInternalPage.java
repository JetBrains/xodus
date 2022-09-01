package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogUtil;
import jetbrains.exodus.tree.ExpiredLoggableCollection;
import jetbrains.exodus.util.MathUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

final class MutableInternalPage extends MutableBasePage<ImmutableInternalPage> {
    final long pageAddress;
    long cachedTreeSize = -1;

    boolean unbalanced;
    MutablePage[] children;

    MutableInternalPage(@NotNull MutableBTree tree, @Nullable ImmutableInternalPage underlying,
                        @NotNull ExpiredLoggableCollection expiredLoggables, @NotNull Log log) {
        super(tree, underlying, expiredLoggables, log);

        if (underlying != null) {
            pageAddress = underlying.address;
            serializedSize = underlying.currentPage.limit() + Long.BYTES;
            this.keyPrefixSize = underlying.getKeyPrefixSize();
        } else {
            pageAddress = -1;
            serializedSize = ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET + ImmutableLeafPage.KEYS_OFFSET +
                    Long.BYTES;
            this.keyPrefixSize = 0;
        }

        if (underlying == null) {
            keys = new ByteBuffer[64];
            children = new MutablePage[64];

            entriesSize = 0;
        }
    }

    MutableInternalPage(@NotNull MutableBTree tree,
                        @NotNull ExpiredLoggableCollection expiredLoggables, @NotNull Log log,
                        ByteBuffer[] keys, MutablePage[] children, int entriesSize,
                        int serializedSize, int keyPrefixSize) {
        super(tree, null, expiredLoggables, log);

        pageAddress = -1;

        this.serializedSize = serializedSize;
        this.keyPrefixSize = keyPrefixSize;
        this.keys = keys;
        this.children = children;
        this.entriesSize = entriesSize;

    }


    MutablePage mutableChild(int index) {
        fetch();

        assert children != null;

        assert index < entriesSize;
        return children[index];
    }


    @Override
    public TraversablePage child(int index) {
        if (underlying != null) {
            return underlying.child(index);
        }

        assert index < entriesSize;
        return children[index];
    }


    @Override
    public boolean isInternalPage() {
        return true;
    }

    @Override
    public ByteBuffer value(int index) {
        throw new UnsupportedOperationException("Internal page can not contain values");
    }


    @Override
    public long save(int structureId, @Nullable MutableInternalPage parent) {
        if (underlying != null) {
            return underlying.address;
        }

        assert serializedSize == serializedSize();
        var newBuffer = LogUtil.allocatePage(serializedSize);

        assert entriesSize >= 2;
        assert newBuffer.limit() <= pageSize || entriesSize < 4;

        byte type;
        if (parent == null) {
            type = ImmutableBTree.INTERNAL_ROOT_PAGE;
        } else {
            type = ImmutableBTree.INTERNAL_PAGE;
        }

        long[] childrenAddresses = new long[entriesSize];
        final int size = entriesSize;
        for (int i = 0; i < size; i++) {
            var child = children[i];
            childrenAddresses[i] = child.save(structureId, this);
        }

        var allocated = log.allocatePage(type, structureId, serializedSize);
        if (allocated != null) {
            var address = allocated.first;
            var buffer = allocated.second;

            serializePage(buffer, childrenAddresses);

            log.finishPageWrite(serializedSize);

            var expired = allocated.third;
            if (expired != null) {
                expiredLoggables.add(expired.firstLong(), expired.secondInt());
            }

            return address;
        } else {
            var buffer = newBuffer.slice(ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET,
                    newBuffer.limit() - ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET);

            serializePage(buffer, childrenAddresses);

            var addressAndExpiredLoggable = log.writeInsideSinglePage(type, structureId, newBuffer, true);
            if (addressAndExpiredLoggable[1] > 0) {
                assert addressAndExpiredLoggable[2] > 0;
                expiredLoggables.add(addressAndExpiredLoggable[1], (int) addressAndExpiredLoggable[2]);
            }

            return addressAndExpiredLoggable[0];
        }
    }

    private void serializePage(ByteBuffer buffer, long[] childAddresses) {
        assert keys != null;

        final int size = entriesSize;
        buffer.order(ByteOrder.nativeOrder());

        //we add Long.BYTES to preserver (sub)tree size
        assert buffer.alignmentOffset(ImmutableBasePage.KEY_PREFIX_LEN_OFFSET + Long.BYTES, Integer.BYTES) == 0;
        buffer.putInt(ImmutableBasePage.KEY_PREFIX_LEN_OFFSET + Long.BYTES, keyPrefixSize);

        assert buffer.alignmentOffset(ImmutableBasePage.ENTRIES_COUNT_OFFSET + Long.BYTES, Integer.BYTES) == 0;
        buffer.putInt(ImmutableBasePage.ENTRIES_COUNT_OFFSET + Long.BYTES, size);

        int keyPositionsOffset = ImmutableBasePage.KEYS_OFFSET + Long.BYTES;
        int childAddressesOffset = keyPositionsOffset + Long.BYTES * size;
        int keysDataOffset = childAddressesOffset + Long.BYTES * size;


        int treeSize = 0;
        for (int i = 0; i < size; i++) {
            var key = keys[i];
            assert key != null;

            var keySize = key.limit();
            var child = children[i];
            assert child != null;

            long subTreeSize = child.treeSize();
            treeSize += subTreeSize;

            assert buffer.alignmentOffset(keyPositionsOffset, Integer.BYTES) == 0;
            assert buffer.alignmentOffset(keyPositionsOffset + Integer.BYTES, Integer.BYTES) == 0;

            buffer.putInt(keyPositionsOffset, keysDataOffset - Long.BYTES);
            buffer.putInt(keyPositionsOffset + Integer.BYTES, keySize);

            assert buffer.alignmentOffset(childAddressesOffset, Long.BYTES) == 0;
            buffer.putLong(childAddressesOffset, childAddresses[i]);

            buffer.put(keysDataOffset, key, 0, keySize);

            keyPositionsOffset += Long.BYTES;
            keysDataOffset += keySize;
            childAddressesOffset += Long.BYTES;
        }

        assert buffer.alignmentOffset(0, Long.BYTES) == 0;
        buffer.putLong(0, treeSize);

        cachedTreeSize = treeSize;
    }


    @Override
    public boolean rebalance(@Nullable MutableInternalPage parent) {
        if (!unbalanced) {
            return false;
        }

        assert keys != null;
        int size = entriesSize;
        for (int i = 0; i < size; i++) {
            var page = children[i];

            var unbalanced = page.rebalance(this);
            if (unbalanced) {
                if (size == 1) {
                    break;
                }

                if (i == 0) {
                    var nextPage = children[i + 1];
                    var nextKey = keys[i + 1];

                    assert nextKey != null;
                    assert nextPage != null;

                    System.arraycopy(keys, i + 2, keys, i + 1, (size - (i + 2)));
                    System.arraycopy(children, i + 2, children, i + 1, (size - (i + 2)));
                    size--;

                    nextPage.rebalance(this);

                    nextPage.fetch();
                    page.merge(nextPage);

                    serializedSize -= entrySize(nextKey);
                } else {
                    var prevPage = children[i - 1];
                    prevPage.merge(page);

                    var removedKey = keys[i];
                    System.arraycopy(keys, i + 1, keys, i, (size - (i + 2)));
                    System.arraycopy(children, i + 1, children, i, (size - (i + 2)));
                    size--;

                    serializedSize -= entrySize(removedKey);

                    //we need to rebalance merged sibling we need to step
                    //one more step back, otherwise because current item
                    //is removed we will process next item
                    i--;
                }

                //because item is removed next item will have the same index
                //so we step back to have the same result after the index
                //increment by cycle
                i--;
            }
        }

        assert serializedSize == serializedSize();
        unbalanced = size < 2 || serializedSize < pageSize / 4;

        return unbalanced;
    }

    @Override
    public void unbalance() {
        unbalanced = true;
    }

    private int serializedSize() {
        assert keys != null;

        int size = ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET + ImmutableLeafPage.KEYS_OFFSET +
                2 * Long.BYTES * entriesSize + Long.BYTES;

        for (int i = 0; i < entriesSize; i++) {
            var key = keys[i];
            assert key != null;

            size += key.limit();
        }

        return size;
    }

    private int entrySize(ByteBuffer key) {
        return 2 * Long.BYTES + key.limit();
    }


    @Override
    public void merge(MutablePage page) {
        fetch();

        assert keys != null;

        var pageToMerge = (MutableInternalPage) page;

        var keysToMerge = pageToMerge.keys;
        var childrenToMerge = pageToMerge.children;
        var entriesToMergeSize = pageToMerge.entriesSize;

        assert keysToMerge != null;

        var resultSize = entriesToMergeSize + entriesSize;

        if (resultSize < keys.length) {
            var newSize = keys.length << 1;

            keys = Arrays.copyOf(keys, newSize);
            children = Arrays.copyOf(children, newSize);
        }

        System.arraycopy(keysToMerge, 0, keys, entriesSize, entriesToMergeSize);
        System.arraycopy(childrenToMerge, 0, children, entriesSize, entriesToMergeSize);

        serializedSize += entriesToMergeSize * 2 * Long.BYTES;

        for (int i = 0; i < entriesToMergeSize; i++) {
            var key = keysToMerge[i];
            assert key != null;

            serializedSize += key.limit();
        }

        entriesSize = resultSize;

        unbalanced = true;
    }

    public void updateFirstKey(ByteBuffer key) {
        fetch();

        assert this.keys != null && entriesSize > 0;

        var currentKey = keys[0];

        assert currentKey != null;

        serializedSize -= currentKey.limit();
        keys[0] = key;
        serializedSize += key.limit();
    }

    @Override
    public boolean split(@Nullable MutableInternalPage parent, int parentIndex, @NotNull ByteBuffer insertedKey,
                         @Nullable ByteBuffer upperBound) {
        assert entriesSize >= 4 && serializedSize > pageSize;

        final int size = entriesSize;

        final int end = size - 2;

        assert keys[0] != null;
        assert keys[1] != null;

        int newSize = ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET +
                ImmutableLeafPage.KEYS_OFFSET +
                2 * Long.BYTES * 2 + Long.BYTES + keys[0].limit() + keys[1].limit();


        int splitAt = 2;
        var threshold = pageSize / 2;
        for (int i = 2; i < end; i++) {
            assert keys[i] != null;

            var nextSize = newSize + entrySize(keys[i]);

            if (nextSize > threshold) {
                break;
            }

            splitAt = i + 1;
            newSize = nextSize;
        }

        final int childEntriesSize = size - splitAt;
        final int childSerializedSize = ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET +
                ImmutableLeafPage.KEYS_OFFSET + Long.BYTES + (serializedSize - newSize);

        final int childEntriesCapacity = Math.max(MathUtil.closestPowerOfTwo(childEntriesSize), 64);
        final ByteBuffer[] childKeys = new ByteBuffer[childEntriesCapacity];
        final MutablePage[] childChildren = new MutablePage[childEntriesCapacity];

        System.arraycopy(keys, splitAt, childKeys, 0, childEntriesSize);
        System.arraycopy(children, splitAt, childChildren, 0, childEntriesSize);

        Arrays.fill(keys, splitAt, size, null);
        Arrays.fill(children, splitAt, size, null);

        entriesSize = splitAt;
        serializedSize = newSize;

        var childPage = new MutableInternalPage(tree, expiredLoggables, log, childKeys, childChildren,
                childEntriesSize, childSerializedSize, keyPrefixSize);

        if (parent == null) {
            parent = new MutableInternalPage(tree, null, expiredLoggables, log);
            parent.addChild(0, keys[0], this);
            parentIndex = 0;

            tree.root = parent;
        }

        var split = parent.addChild(parentIndex + 1,
                generateParentKey(parent, insertedKey, keyPrefixSize, childKeys[0]), childPage);
        parent.updatePrefixSize(parentIndex + 1, upperBound);

        assert serializedSize == serializedSize();
        assert childPage.serializedSize == childPage.serializedSize();

        return split;
    }


    boolean addChild(int index, ByteBuffer key, MutablePage page) {
        fetch();

        assert key != null;

        if (entriesSize + 1 > keys.length) {
            var newSize = keys.length << 1;

            keys = Arrays.copyOf(keys, newSize);
            children = Arrays.copyOf(children, newSize);
        }

        serializedSize += entrySize(key);

        var size = entriesSize;

        if (index < size) {
            System.arraycopy(keys, index, keys, index + 1, size - index);
            System.arraycopy(children, index, children, index + 1, size - index);
        }

        keys[index] = key;
        children[index] = page;

        entriesSize++;

        return entriesSize > 4 && serializedSize > pageSize;
    }

    void updatePrefixSize(int index, ByteBuffer upperBoundary) {
        assert index > 0;

        updateCommonPrefix(index - 1, keys[index]);
        if (index < entriesSize - 1) {
            updateCommonPrefix(index, keys[index + 1]);
        } else {
            if (upperBoundary != null) {
                if (upperBoundary.remaining() > 0) {
                    updateCommonPrefix(index, upperBoundary);
                }
            }
        }
    }

    private void updateCommonPrefix(int index, ByteBuffer nextKey) {
        var key = keys[index];

        var commonPrefixSize = keyPrefixSize + MutableBTree.commonPrefix(key, nextKey);
        var child = children[index];
        var childKeyPrefixSize = child.getKeyPrefixSize();

        if (childKeyPrefixSize < commonPrefixSize) {
            child.truncateKeys(commonPrefixSize - childKeyPrefixSize);
        }
    }

    public boolean fetch() {
        if (underlying == null) {
            return false;
        }

        expiredLoggables.add(pageAddress, pageSize);

        final int size = underlying.getEntriesCount();
        final int capacity = Math.max(MathUtil.closestPowerOfTwo(size), 64);

        keys = new ByteBuffer[capacity];
        children = new MutablePage[capacity];
        entriesSize = size;

        for (int i = 0; i < size; i++) {
            var key = underlying.key(i);
            var child = underlying.child(i);

            keys[i] = key;
            children[i] = child.toMutable(tree, expiredLoggables);
        }

        underlying = null;
        return true;
    }

    @Override
    public long treeSize() {
        if (underlying != null) {
            return underlying.getTreeSize();
        }

        assert keys != null;

        if (cachedTreeSize >= 0) {
            return cachedTreeSize;
        }

        int treeSize = 0;
        final int size = entriesSize;
        for (int i = 0; i < size; i++) {
            treeSize += children[i].treeSize();
        }

        return treeSize;
    }
}