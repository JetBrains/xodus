package jetbrains.exodus.tree.ibtree;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import jetbrains.exodus.ByteBufferByteIterable;
import jetbrains.exodus.ByteBufferComparator;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogUtil;
import jetbrains.exodus.tree.ExpiredLoggableCollection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.Collections;
import java.util.RandomAccess;

final class MutableLeafPage implements MutablePage {
    @Nullable
    ImmutableLeafPage underlying;
    @Nullable
    ObjectArrayList<Entry> changedEntries;

    @NotNull
    final KeyView keyView;
    @NotNull
    final ValueView valueView;

    @NotNull
    final Log log;

    final int pageSize;

    final int maxKeySize;

    @Nullable
    MutableInternalPage parent;
    @NotNull
    final ExpiredLoggableCollection expiredLoggables;

    final long pageAddress;

    boolean unbalanced;

    ByteBufferHolder firstKey;

    MutableLeafPage(@Nullable ImmutableLeafPage underlying, @NotNull Log log, int pageSize,
                    @NotNull ExpiredLoggableCollection expiredLoggables,
                    @Nullable MutableInternalPage parent) {
        this.underlying = underlying;
        if (underlying != null) {
            this.pageAddress = underlying.address;
        } else {
            this.pageAddress = -1;
        }

        this.log = log;
        this.pageSize = pageSize;
        this.parent = parent;
        this.expiredLoggables = expiredLoggables;

        if (underlying == null) {
            changedEntries = new ObjectArrayList<>();
        }

        keyView = new KeyView();
        valueView = new ValueView();

        maxKeySize = pageSize / 4;
    }

    @Override
    public ByteBuffer key(int index) {
        return keyView.get(index);
    }

    @Override
    public int find(ByteBuffer key) {
        return Collections.binarySearch(keyView, key, ByteBufferComparator.INSTANCE);
    }

    void set(int index, ByteBuffer key, ByteBuffer value) {
        fetch();

        assert changedEntries != null;
        var entry = changedEntries.get(index);

        if (entry != null) {
            entry.key.addExpiredLoggable(expiredLoggables);
            entry.value.addExpiredLoggable(expiredLoggables);
        }

        changedEntries.set(index, new Entry(new LoadedByteBufferHolder(key, maxKeySize),
                new LoadedByteBufferHolder(value, 0)));
    }

    void insert(int index, ByteBuffer key, ByteBuffer value) {
        fetch();

        assert changedEntries != null;
        changedEntries.add(index, new Entry(new LoadedByteBufferHolder(key, maxKeySize),
                new LoadedByteBufferHolder(value, 0)));
    }

    void append(ByteBuffer key, ByteBuffer value) {
        fetch();

        assert changedEntries != null;
        changedEntries.add(new Entry(new LoadedByteBufferHolder(key, maxKeySize),
                new LoadedByteBufferHolder(value, 0)));
    }

    void delete(int index) {
        fetch();

        assert changedEntries != null;
        var entry = changedEntries.remove(index);

        entry.key.addExpiredLoggable(expiredLoggables);
        entry.value.addExpiredLoggable(expiredLoggables);

        unbalanced = true;
    }

    @Override
    public long save(int structureId) {
        if (changedEntries == null) {
            assert underlying != null;
            return underlying.address;
        }

        var newBuffer = LogUtil.allocatePage(pageSize);
        var buffer = newBuffer.slice(ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET,
                        pageSize - ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET).
                order(ByteOrder.nativeOrder());

        assert buffer.alignmentOffset(ImmutableBasePage.KEY_PREFIX_LEN_OFFSET, Integer.BYTES) == 0;
        buffer.putInt(ImmutableBasePage.KEY_PREFIX_LEN_OFFSET, 0);

        assert buffer.alignmentOffset(ImmutableBasePage.ENTRIES_COUNT_OFFSET, Integer.BYTES) == 0;
        buffer.putInt(ImmutableBasePage.ENTRIES_COUNT_OFFSET, changedEntries.size());

        int keyDataOffset = ImmutableBasePage.KEYS_OFFSET + changedEntries.size() * 2 * Long.BYTES;
        int keysOffset = ImmutableBasePage.KEYS_OFFSET;
        int valueAddressesOffset = ImmutableBasePage.KEYS_OFFSET + Long.BYTES * changedEntries.size();

        for (var entry : changedEntries) {
            var key = entry.key;
            var value = entry.value;

            var keyAddress = key.getAddress();
            if (keyAddress == 0) {
                var keyBuffer = key.getByteBuffer();

                if (keyBuffer.limit() <= maxKeySize) {
                    keyAddress = ((long) keyBuffer.limit() << Integer.SIZE) | keyDataOffset;
                    buffer.put(keyDataOffset, keyBuffer, 0, keyBuffer.limit());
                    keyDataOffset += keyBuffer.limit();
                } else {
                    keyAddress -= log.write(ImmutableBTree.KEY_NODE, structureId, new ByteBufferByteIterable(keyBuffer));
                }
            }

            assert buffer.alignmentOffset(keysOffset, Long.BYTES) == 0;
            buffer.putLong(keysOffset, keyAddress);
            keysOffset += Long.BYTES;

            var valueAddress = value.getAddress();
            if (valueAddress == 0) {
                var valueByteBuffer = value.getByteBuffer();
                valueAddress = log.write(ImmutableBTree.VALUE_NODE, structureId, new ByteBufferByteIterable(valueByteBuffer));
            }

            assert buffer.alignmentOffset(valueAddressesOffset, Long.BYTES) == 0;

            buffer.putLong(valueAddressesOffset, valueAddress);
            valueAddressesOffset += Long.BYTES;
        }

        return log.writeNewPage(newBuffer);
    }

    @Override
    public MutablePage rebalance() {
        if (unbalanced) {
            unbalanced = false;

            var threshold = pageSize / 4;

            assert changedEntries != null;

            // Ignore if node is above threshold (25%) and contains at
            if (serializedSize() < threshold || changedEntries.isEmpty()) {
                // Root node has special handling.
                if (parent == null) {
                    return null;
                }

                // If node has no keys then just remove it.
                if (changedEntries.isEmpty()) {
                    final int childIndex = parent.find(firstKey.getByteBuffer());
                    assert childIndex >= 0;

                    parent.delete(childIndex);
                    return null;
                }

                assert parent.numChildren() > 1 : "parent must have at least 2 children";

                // Destination node is right sibling if idx == 0, otherwise left sibling.
                // If both this node and the target node are too small then merge them.
                var parentIndex = parent.find(firstKey.getByteBuffer());
                if (parentIndex == 0) {
                    var nextSibling = (MutableLeafPage) parent.mutableChild(1);
                    nextSibling.fetch();

                    changedEntries.addAll(nextSibling.changedEntries);
                    parent.delete(1);
                } else {
                    var prevSibling = (MutableLeafPage) parent.mutableChild(parentIndex - 1);
                    prevSibling.fetch();

                    assert prevSibling.changedEntries != null;

                    prevSibling.changedEntries.addAll(changedEntries);
                    parent.delete(parentIndex);
                }
            }
        }

        return null;
    }

    private int serializedSize() {
        assert changedEntries != null;

        int size = ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET +
                ImmutableLeafPage.KEYS_OFFSET + 2 * Long.BYTES * changedEntries.size();

        for (Entry entry : changedEntries) {
            size += entry.key.embeddedSize();
        }

        return size;
    }

    @Override
    public void spill() {
        if (changedEntries == null) {
            return;
        }

        var page = this;

        while (true) {
            var nextSiblingEntries = page.splitAtPageSize();

            if (nextSiblingEntries == null) {
                break;
            }

            if (parent == null) {
                parent = new MutableInternalPage(null, expiredLoggables,
                        log, pageSize,
                        null);
            }

            page = new MutableLeafPage(null, log, pageSize,
                    expiredLoggables, parent);

            page.changedEntries = nextSiblingEntries;
            page.firstKey = nextSiblingEntries.get(0).key;

            parent.addChild(page.firstKey, page);
            parent.sortBeforeInternalSpill = true;
        }

        //parent first spill children then itself
        //so we do not need sort children of parent or spill parent itself
    }


    private ObjectArrayList<Entry> splitAtPageSize() {
        assert changedEntries != null;

        //root can contain 0 pages, leaf page should keep at least one entry
        if (changedEntries.size() <= 1) {
            return null;
        }

        var firstEntry = changedEntries.get(0);
        int size = ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET +
                ImmutableLeafPage.KEYS_OFFSET + 2 * Long.BYTES + firstEntry.key.embeddedSize();

        int indexToSplit = 0;


        for (int i = 1; i < changedEntries.size(); i++) {
            var entry = changedEntries.get(i);

            size += 2 * Long.BYTES + entry.key.embeddedSize();
            if (size > pageSize) {
                break;
            }

            indexToSplit = 1;
        }

        ObjectArrayList<Entry> result = null;

        if (indexToSplit < changedEntries.size() - 1) {
            result = new ObjectArrayList<>();
            result.addAll(0, changedEntries.subList(indexToSplit + 1, changedEntries.size()));

            changedEntries.removeElements(indexToSplit + 1, changedEntries.size());
        }

        return result;
    }

    private void fetch() {
        //already fetched
        if (underlying == null) {
            return;
        }

        expiredLoggables.add(pageAddress, pageSize);

        final int size = underlying.getEntriesCount();
        changedEntries = new ObjectArrayList<>(size);

        for (int i = 0; i < size; i++) {
            var keyAddress = underlying.getKeyAddress(i);
            ByteBufferHolder keyHolder;

            if (keyAddress < 0) {
                keyHolder = new LazyByteBufferHolder(log, -keyAddress);
            } else {
                var key = underlying.getEmbeddedKey(keyAddress);
                keyHolder = new LoadedByteBufferHolder(key, maxKeySize);
            }

            var valueHolder = new LazyByteBufferHolder(log, underlying.getChildAddress(i));
            changedEntries.add(new Entry(keyHolder, valueHolder));
        }

        firstKey = changedEntries.get(0).key;

        //do not keep copy of the data for a long time
        underlying = null;
    }

    @Override
    public long treeSize() {
        return keyView.size();
    }

    private static final class Entry implements Comparable<Entry> {
        ByteBufferHolder key;
        ByteBufferHolder value;

        Entry(ByteBufferHolder key, ByteBufferHolder value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo(@NotNull MutableLeafPage.Entry entry) {
            return ByteBufferComparator.INSTANCE.compare(key.getByteBuffer(), entry.key.getByteBuffer());
        }
    }

    final class ValueView extends AbstractList<ByteBuffer> implements RandomAccess {
        @Override
        public ByteBuffer get(int i) {
            if (changedEntries != null) {
                return changedEntries.get(i).value.getByteBuffer();
            }

            assert underlying != null;
            return underlying.getValue(i);
        }

        @Override
        public int size() {
            if (changedEntries != null) {
                return changedEntries.size();
            }

            assert underlying != null;
            return underlying.getEntriesCount();
        }
    }

    final class KeyView extends AbstractList<ByteBuffer> implements RandomAccess {
        @Override
        public ByteBuffer get(int i) {
            if (changedEntries != null) {
                return changedEntries.get(i).key.getByteBuffer();
            }

            assert underlying != null;
            return underlying.key(i);
        }

        @Override
        public int size() {
            if (changedEntries != null) {
                return changedEntries.size();
            }

            assert underlying != null;
            return underlying.getEntriesCount();
        }
    }
}


