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

final class MutableLeafPage extends MutableBasePage<ImmutableLeafPage> {
    final long pageAddress;

    boolean unbalanced;

    @Nullable
    ByteBuffer[] values;

    MutableLeafPage(@NotNull MutableBTree tree, @Nullable ImmutableLeafPage underlying,
                    @NotNull Log log,
                    @NotNull ExpiredLoggableCollection expiredLoggables) {
        super(tree, underlying, expiredLoggables, log);

        if (underlying != null) {
            this.pageAddress = underlying.address;
            this.serializedSize = underlying.page.limit() + ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET;
            this.keyPrefix = underlying.keyPrefix();
        } else {
            this.pageAddress = -1;
            this.serializedSize = ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET +
                    ImmutableLeafPage.ENTRY_POSITIONS_OFFSET;
            this.keyPrefix = null;
        }

        if (underlying == null) {
            keys = new ByteBuffer[64];
            values = new ByteBuffer[64];
            entriesSize = 0;
        }
    }

    MutableLeafPage(@NotNull MutableBTree tree,
                    @NotNull Log log,
                    @NotNull ExpiredLoggableCollection expiredLoggables, int serializedSize, ByteBuffer keyPrefix,
                    int entriesSize, ByteBuffer[] keys, ByteBuffer[] values) {
        super(tree, null, expiredLoggables, log);

        this.pageAddress = -1;
        this.serializedSize = serializedSize;
        this.keyPrefix = keyPrefix;
        this.entriesSize = entriesSize;
        this.keys = keys;
        this.values = values;
    }

    @Override
    public TraversablePage child(int index) {
        throw new UnsupportedOperationException("Leaf pages do not contain children");
    }

    @Override
    public boolean isInternalPage() {
        return false;
    }

    @Override
    public ByteBuffer value(int index) {
        if (underlying != null) {
            return underlying.value(index);
        }

        assert index < entriesSize;
        return values[index];
    }

    boolean set(int index, ByteBuffer key, ByteBuffer value) {
        fetch();

        assert keys != null;

        assert index < entriesSize;

        var prevValue = values[index];
        var prevKey = keys[index];

        keys[index] = key;
        values[index] = value;

        serializedSize += key.limit() + value.limit() - prevValue.limit() - prevKey.limit();

        return entriesSize > 1 && serializedSize > pageSize;
    }

    boolean insert(int index, ByteBuffer key, ByteBuffer value) {
        fetch();

        assert keys != null;

        final int size = entriesSize;

        ensureCapacity(size + 1);

        if (index < size) {
            System.arraycopy(keys, index, keys, index + 1, size - index);
            System.arraycopy(values, index, values, index + 1, size - index);
        }

        keys[index] = key;
        values[index] = value;

        serializedSize += entrySize(key, value);
        entriesSize++;

        return entriesSize > 1 && serializedSize > pageSize;
    }

    boolean append(ByteBuffer key, ByteBuffer value) {
        fetch();

        assert keys != null;
        final int size = entriesSize;

        ensureCapacity(size + 1);

        keys[size] = key;
        values[size] = value;

        serializedSize += entrySize(key, value);
        entriesSize++;

        return entriesSize > 1 && serializedSize > pageSize;
    }

    private int entrySize(ByteBuffer key, ByteBuffer value) {
        return 2 * Long.BYTES + key.limit() + value.limit();
    }

    private void ensureCapacity(int size) {
        if (size > keys.length) {
            keys = Arrays.copyOf(keys, keys.length << 1);
            values = Arrays.copyOf(values, values.length << 1);
        }
    }

    void delete(int index) {
        fetch();

        assert keys != null;

        final int size = entriesSize;

        var prevKey = keys[index];
        var prevValue = values[index];

        System.arraycopy(keys, index + 1, keys, index, size - (index + 1));
        System.arraycopy(values, index + 1, values, index, size - (index + 1));

        entriesSize--;
        serializedSize -= (prevValue.limit() + prevKey.limit() + 2 * Long.BYTES);

        unbalanced = true;
    }

    @Override
    public void unbalance() {
        unbalanced = true;
    }

    @Override
    public long save(int structureId, @Nullable MutableInternalPage parent) {
        if (underlying != null) {
            return underlying.address;
        }

        assert parent == null || entriesSize >= 1;

        assert serializedSize() == serializedSize;
        assert serializedSize <= pageSize || entriesSize < 2;


        byte type;
        if (parent == null) {
            type = ImmutableBTree.LEAF_ROOT_PAGE;
        } else {
            type = ImmutableBTree.LEAF_PAGE;
        }
        var allocated = log.allocatePage(type, structureId, serializedSize);
        if (allocated != null) {
            var address = allocated.first;
            var buffer = allocated.second;

            serializePage(buffer);

            log.finishPageWrite(serializedSize);

            var expired = allocated.third;
            if (expired != null) {
                expiredLoggables.add(expired.firstLong(), expired.secondInt());
            }

            return address;
        } else {
            var newBuffer = LogUtil.allocatePage(serializedSize);
            var buffer = newBuffer.slice(ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET,
                    newBuffer.limit() - ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET);

            serializePage(buffer);

            var addressAndExpiredLoggable = log.writeInsideSinglePage(type, structureId, newBuffer, true);
            if (addressAndExpiredLoggable[1] > 0) {
                assert addressAndExpiredLoggable[2] > 0;
                expiredLoggables.add(addressAndExpiredLoggable[1], (int) addressAndExpiredLoggable[2]);
            }

            return addressAndExpiredLoggable[0];
        }
    }

    private void serializePage(ByteBuffer buffer) {
        assert keys != null;

        final int size = entriesSize;
        buffer.order(ByteOrder.nativeOrder());

        assert buffer.alignmentOffset(ImmutableBasePage.ENTRIES_COUNT_OFFSET, Integer.BYTES) == 0;
        buffer.putInt(ImmutableBasePage.ENTRIES_COUNT_OFFSET, size);

        var keyPrefixSize = getKeyPrefixSize();
        assert buffer.alignmentOffset(ImmutableBasePage.KEY_PREFIX_LEN_OFFSET, Integer.BYTES) == 0;
        buffer.putInt(ImmutableBasePage.KEY_PREFIX_LEN_OFFSET, keyPrefixSize);

        if (keyPrefixSize > 0) {
            buffer.put(ImmutableBasePage.ENTRY_POSITIONS_OFFSET + 2 * Long.BYTES * size,
                    keyPrefix, 0, keyPrefixSize);
        }

        int keysPositionsOffset = ImmutableBasePage.ENTRY_POSITIONS_OFFSET;
        int keysDataOffset = ImmutableBasePage.ENTRY_POSITIONS_OFFSET + keyPrefixSize + size * 2 * Long.BYTES;

        int valuesPositionsOffset = ImmutableBasePage.ENTRY_POSITIONS_OFFSET + Long.BYTES * size;
        int valuesDataOffset = serializedSize - ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET;

        for (int i = 0; i < size; i++) {
            var key = keys[i];
            var value = values[i];

            assert key != null;
            assert value != null;

            var keyPosition = keysDataOffset;
            var keySize = key.limit();

            var valueSize = value.limit();
            var valuePosition = valuesDataOffset - valueSize;

            valuesDataOffset = valuePosition;

            assert buffer.alignmentOffset(keysPositionsOffset, Integer.BYTES) == 0;
            assert buffer.alignmentOffset(keysPositionsOffset + Integer.BYTES, Integer.BYTES) == 0;

            assert buffer.alignmentOffset(valuesPositionsOffset, Integer.BYTES) == 0;
            assert buffer.alignmentOffset(valuesPositionsOffset + Integer.BYTES, Integer.BYTES) == 0;

            buffer.putInt(keysPositionsOffset, keyPosition);
            buffer.putInt(keysPositionsOffset + Integer.BYTES, keySize);

            buffer.putInt(valuesPositionsOffset, valuePosition);
            buffer.putInt(valuesPositionsOffset + Integer.BYTES, valueSize);

            buffer.put(keysDataOffset, key, 0, keySize);
            buffer.put(valuesDataOffset, value, 0, valueSize);

            keysPositionsOffset += Long.BYTES;
            valuesPositionsOffset += Long.BYTES;
            keysDataOffset += keySize;
        }
    }

    @Override
    public boolean rebalance(@Nullable MutableInternalPage parent) {
        if (!unbalanced) {
            return false;
        }

        assert keys != null;
        assert serializedSize == serializedSize();

        unbalanced = entriesSize == 0 || serializedSize < pageSize / 4;
        return unbalanced;
    }

    @Override
    public void merge(MutablePage page) {
        fetch();

        assert keys != null;

        var leafPageToMerge = (MutableLeafPage) page;
        assert leafPageToMerge.keys != null;

        var leafPageToMergeKeys = leafPageToMerge.keys;
        var leafPageToMergeValues = leafPageToMerge.values;

        var leafPageToMergeEntriesSize = leafPageToMerge.entriesSize;

        serializedSize += 2 * Long.BYTES * leafPageToMergeEntriesSize;

        for (int i = 0; i < leafPageToMergeEntriesSize; i++) {
            serializedSize += leafPageToMergeKeys[i].limit() + leafPageToMergeValues[i].limit();
        }

        var mergedSize = leafPageToMergeEntriesSize + entriesSize;
        ensureCapacity(mergedSize);

        var size = entriesSize;

        System.arraycopy(leafPageToMergeKeys, 0, keys, size, leafPageToMergeEntriesSize);
        System.arraycopy(leafPageToMergeValues, 0, values, size, leafPageToMergeEntriesSize);

        entriesSize = mergedSize;

        assert serializedSize == serializedSize();

        unbalanced = true;
    }

    private int serializedSize() {
        assert keys != null;

        final int entries = entriesSize;
        int size = ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET +
                ImmutableLeafPage.ENTRY_POSITIONS_OFFSET + getKeyPrefixSize() + 2 * Long.BYTES * entries;

        for (int i = 0; i < entries; i++) {
            var key = keys[i];
            var value = values[i];

            assert key != null;
            assert value != null;

            size += key.limit();
            size += value.limit();
        }

        return size;
    }


    @Override
    public boolean split(@Nullable MutableInternalPage parent, int parentIndex, @NotNull ByteBuffer insertedKey,
                         @Nullable ByteBuffer upperBound) {
        assert entriesSize >= 2 && serializedSize > pageSize;

        final int size = entriesSize;

        final int end = size - 1;
        var keyPrefixSize = getKeyPrefixSize();

        final int prefixSize = ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET +
                ImmutableLeafPage.ENTRY_POSITIONS_OFFSET + keyPrefixSize;

        assert keys[0] != null;
        assert values[0] != null;

        int newSize = keys[0].limit() + values[0].limit() + 2 * Long.BYTES;
        int splitAt = 1;

        var threshold = (pageSize - prefixSize) / 2;
        for (int i = 1; i < end; i++) {
            assert keys[i] != null;
            assert values[i] != null;

            var nextSize = newSize + entrySize(keys[i], values[i]);

            if (nextSize > threshold) {
                break;
            }

            splitAt = i + 1;
            newSize = nextSize;
        }

        final int childEntriesSize = size - splitAt;
        final int childSerializedSize = serializedSize - newSize;

        final int childEntriesCapacity = Math.max(MathUtil.closestPowerOfTwo(childEntriesSize), 64);

        final ByteBuffer[] childKeys = new ByteBuffer[childEntriesCapacity];
        final ByteBuffer[] childValues = new ByteBuffer[childEntriesCapacity];

        System.arraycopy(keys, splitAt, childKeys, 0, childEntriesSize);
        System.arraycopy(values, splitAt, childValues, 0, childEntriesSize);

        Arrays.fill(keys, splitAt, size, null);
        Arrays.fill(values, splitAt, size, null);

        entriesSize = splitAt;
        serializedSize = prefixSize + newSize;

        if (parent == null) {
            parent = new MutableInternalPage(tree, null, expiredLoggables, log);
            parent.addChild(0, keys[0], this);
            parentIndex = 0;

            tree.root = parent;
        }

        var childPage = new MutableLeafPage(tree, log, expiredLoggables, childSerializedSize, keyPrefix,
                childEntriesSize, childKeys, childValues);

        var split = parent.addChild(parentIndex + 1,
                generateParentKey(parent, insertedKey, keyPrefixSize, childKeys[0]), childPage);

        assert serializedSize == serializedSize();
        assert childPage.serializedSize == childPage.serializedSize();

        parent.updatePrefixSize(parentIndex + 1, upperBound);

        assert serializedSize == serializedSize();
        assert childPage.serializedSize == childPage.serializedSize();

        return split;
    }

    public boolean fetch() {
        //already fetched
        if (underlying == null) {
            return false;
        }

        expiredLoggables.add(pageAddress, pageSize);

        final int size = underlying.getEntriesCount();
        final int capacity = Math.max(MathUtil.closestPowerOfTwo(size), 64);

        keys = new ByteBuffer[capacity];
        values = new ByteBuffer[capacity];
        entriesSize = size;

        for (int i = 0; i < size; i++) {
            var key = underlying.key(i);
            keys[i] = key;
        }

        for (int i = 0; i < size; i++) {
            var value = underlying.value(i);
            values[i] = value;
        }

        //do not keep copy of the data for a long time
        underlying = null;

        return true;
    }

    @Override
    public long treeSize() {
        if (underlying != null) {
            return underlying.getTreeSize();
        }

        return entriesSize;
    }
}


