package jetbrains.exodus.tree.ibtree;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
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
    final int pageOffset;

    @Nullable
    MutableInternalPage parent;
    @NotNull
    final ExpiredLoggableCollection expiredLoggables;

    boolean unbalanced;

    ByteBuffer firstKey;

    MutableLeafPage(@Nullable ImmutableLeafPage underlying, @NotNull Log log, int pageSize,
                    final int pageOffset,
                    @NotNull ExpiredLoggableCollection expiredLoggables,
                    @Nullable MutableInternalPage parent) {
        this.underlying = underlying;
        this.log = log;
        this.pageSize = pageSize;
        this.parent = parent;
        this.pageOffset = pageOffset;
        this.expiredLoggables = expiredLoggables;

        keyView = new KeyView();
        valueView = new ValueView();

        //page should contain at least single key
        firstKey = keyView.get(0);
    }

    @Override
    public ByteBuffer key(int index) {
        return keyView.get(index);
    }

    ByteBuffer value(int index) {
        return valueView.get(index);
    }

    @Override
    public int find(ByteBuffer key) {
        return Collections.binarySearch(keyView, key, ByteBufferComparator.INSTANCE);
    }

    void set(int index, ByteBuffer key, ByteBuffer value) {
        fetch();

        assert changedEntries != null;
        changedEntries.set(index, new Entry(key, value));
    }

    void append(ByteBuffer key, ByteBuffer value) {
        fetch();

        assert changedEntries != null;
        changedEntries.add(new Entry(key, value));
    }

    void delete(int index) {
        fetch();

        assert changedEntries != null;
        changedEntries.remove(index);

        unbalanced = true;
    }

    @Override
    public long save(int structureId) {
        if (changedEntries == null) {
            assert underlying != null;
            return underlying.pageIndex;
        }

        var newBuffer = LogUtil.allocatePage(pageSize);
        var buffer = newBuffer.slice(pageOffset, pageSize - pageOffset).
                order(ByteOrder.nativeOrder());

        assert buffer.alignmentOffset(ImmutableBasePage.KEY_PREFIX_LEN_OFFSET, Integer.BYTES) == 0;
        buffer.putInt(ImmutableBasePage.KEY_PREFIX_LEN_OFFSET, 0);

        assert buffer.alignmentOffset(ImmutableBasePage.ENTRIES_COUNT_OFFSET, Integer.BYTES) == 0;
        buffer.putInt(ImmutableBasePage.ENTRIES_COUNT_OFFSET, changedEntries.size());

        int keyValueEntryOffset = ImmutableLeafPage.KEYS_OFFSET +
                (ImmutableLeafPage.KEY_ENTRY_SIZE + ImmutableLeafPage.VALUE_ENTRY_SIZE) * changedEntries.size();

        int keyPositionSizeOffset = ImmutableLeafPage.KEYS_OFFSET;
        int valuePositionSizeOffset = keyPositionSizeOffset +
                changedEntries.size() * ImmutableLeafPage.KEY_ENTRY_SIZE;

        final int startEntryOffset = keyValueEntryOffset;
        int size = pageOffset + keyValueEntryOffset;

        for (var entry : changedEntries) {
            assert buffer.alignmentOffset(keyPositionSizeOffset, Integer.BYTES) == 0;
            buffer.putInt(keyPositionSizeOffset, keyValueEntryOffset);
            keyPositionSizeOffset += ImmutableLeafPage.KEY_POSITION_SIZE;

            assert buffer.alignmentOffset(keyPositionSizeOffset, Integer.BYTES) == 0;

            final int keySize = entry.key.limit();
            buffer.putInt(keyPositionSizeOffset, keySize);
            keyPositionSizeOffset += ImmutableLeafPage.KEY_SIZE_SIZE;

            buffer.put(keyValueEntryOffset, entry.key, 0, keySize);
            keyValueEntryOffset += keySize;

            assert buffer.alignmentOffset(valuePositionSizeOffset, Integer.BYTES) == 0;
            buffer.putInt(valuePositionSizeOffset, keyValueEntryOffset);
            valuePositionSizeOffset += ImmutableLeafPage.VALUE_POSITION_SIZE;

            assert buffer.alignmentOffset(valuePositionSizeOffset, Integer.BYTES) == 0;
            final int valueSize = entry.value.limit();
            buffer.putInt(valuePositionSizeOffset, valueSize);
            valuePositionSizeOffset += ImmutableLeafPage.VALUE_SIZE_SIZE;

            buffer.put(keyValueEntryOffset, entry.value, 0, valueSize);
            keyValueEntryOffset += valueSize;
        }

        size += (keyValueEntryOffset - startEntryOffset);
        final int pages;

        if (size + pageOffset <= pageSize) {
            pages = 1;
        } else {
            pages = (((size - (pageSize - pageOffset)) + pageSize - 1) / pageSize) + 1;
        }

        assert buffer.alignmentOffset(ImmutableLeafPage.PAGES_COUNT_OFFSET, Short.BYTES) == 0;
        buffer.putShort(ImmutableLeafPage.PAGES_COUNT_OFFSET, (short) pages);

        return log.writeNewPage(newBuffer);
    }

    @Override
    public void rebalance() {
        if (unbalanced) {
            unbalanced = false;

            var threshold = pageSize / 4;

            assert changedEntries != null;

            // Ignore if node is above threshold (25%)
            if (serializedSize() < threshold) {
                // Root node has special handling.
                if (parent == null) {
                    return;
                }

                // If node has no keys then just remove it.
                if (changedEntries.isEmpty()) {
                    final int childIndex = parent.find(firstKey);
                    assert childIndex >= 0;

                    parent.delete(childIndex);
                    parent.rebalance();
                    return;
                }

                assert parent.numChildren() > 1 : "parent must have at least 2 children";

                // Destination node is right sibling if idx == 0, otherwise left sibling.
                // If both this node and the target node are too small then merge them.
                var parentIndex = parent.find(firstKey);
                if (parentIndex == 0) {
                    var nextSibling = (MutableLeafPage) parent.child(1);
                    nextSibling.fetch();

                    changedEntries.addAll(nextSibling.changedEntries);
                    parent.delete(1);
                } else {
                    var prevSibling = (MutableLeafPage) parent.child(parentIndex - 1);
                    prevSibling.fetch();

                    assert prevSibling.changedEntries != null;

                    prevSibling.changedEntries.addAll(changedEntries);
                    parent.delete(parentIndex);
                }

                parent.rebalance();
            }
        }
    }

    private int serializedSize() {
        assert changedEntries != null;

        int size = pageOffset +
                ImmutableLeafPage.KEYS_OFFSET +
                (ImmutableLeafPage.KEY_ENTRY_SIZE + ImmutableLeafPage.VALUE_ENTRY_SIZE) * changedEntries.size();

        for (Entry entry : changedEntries) {
            size += entry.key.remaining();
            size += entry.value.remaining();
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
                        log, pageSize, 24,
                        null);
            }

            page = new MutableLeafPage(null, log, pageSize, 16,
                    expiredLoggables, parent);

            parent.addChild(page.firstKey, nextSiblingEntries.size(), page);
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
        int size = pageOffset + ImmutableLeafPage.KEYS_OFFSET + ImmutableLeafPage.KEY_ENTRY_SIZE +
                ImmutableLeafPage.VALUE_ENTRY_SIZE + firstEntry.key.limit() + firstEntry.value.limit();

        int indexToSplit = 0;

        for (int i = 1; i < changedEntries.size(); i++) {
            var entry = changedEntries.get(i);

            size += entry.key.limit() + entry.value.limit() + ImmutableLeafPage.KEY_ENTRY_SIZE +
                    ImmutableLeafPage.VALUE_ENTRY_SIZE;
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

        expiredLoggables.add(underlying.pageIndex * pageSize,
                Short.toUnsignedInt(underlying.getPagesCount()) * pageSize);

        final int size = underlying.getEntriesCount();
        changedEntries = new ObjectArrayList<>(size);

        for (int i = 0; i < size; i++) {
            final ByteBuffer key = underlying.getKeyUnsafe(i);
            final ByteBuffer value = underlying.getValueUnsafe(i);

            changedEntries.add(new Entry(key, value));
        }

        firstKey = changedEntries.get(0).key;

        //do not keep copy of the data for a long time
        underlying = null;
    }

    private static final class Entry implements Comparable<Entry> {
        ByteBuffer key;
        ByteBuffer value;

        Entry(ByteBuffer key, ByteBuffer value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo(@NotNull MutableLeafPage.Entry entry) {
            return ByteBufferComparator.INSTANCE.compare(key, entry.key);
        }
    }

    final class ValueView extends AbstractList<ByteBuffer> implements RandomAccess {
        @Override
        public ByteBuffer get(int i) {
            if (changedEntries != null) {
                return changedEntries.get(i).value;
            }

            assert underlying != null;
            return underlying.value(i);
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
                return changedEntries.get(i).key;
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


