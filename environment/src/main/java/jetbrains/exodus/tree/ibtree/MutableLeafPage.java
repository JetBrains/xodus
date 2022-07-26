package jetbrains.exodus.tree.ibtree;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import jetbrains.exodus.ByteBufferComparator;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogUtil;
import jetbrains.exodus.log.Loggable;
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

    @Nullable
    MutableInternalPage parent;
    @NotNull
    final ExpiredLoggableCollection expiredLoggables;

    final long pageAddress;

    boolean unbalanced;

    ByteBuffer firstKey;

    @NotNull
    final MutableBTree tree;
    boolean spilled;

    MutableLeafPage(@NotNull MutableBTree tree, @Nullable ImmutableLeafPage underlying,
                    @NotNull Log log, int pageSize,
                    @NotNull ExpiredLoggableCollection expiredLoggables,
                    @Nullable MutableInternalPage parent) {
        this.tree = tree;
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
    }

    @Override
    public ByteBuffer key(int index) {
        return keyView.get(index);
    }

    @Override
    public int getEntriesCount() {
        return keyView.size();
    }

    @Override
    public TraversablePage child(int index) {
        throw new UnsupportedOperationException("Leaf pages do not contain children");
    }

    @Override
    public int find(ByteBuffer key) {
        return Collections.binarySearch(keyView, key, ByteBufferComparator.INSTANCE);
    }

    @Override
    public boolean isInternalPage() {
        return false;
    }

    @Override
    public ByteBuffer value(int index) {
        return valueView.get(index);
    }

    void set(int index, ByteBuffer key, ByteBuffer value) {
        fetch();

        assert changedEntries != null;
        changedEntries.set(index, new Entry(key, value));
    }

    void insert(int index, ByteBuffer key, ByteBuffer value) {
        fetch();

        assert changedEntries != null;
        changedEntries.add(index, new Entry(key, value));
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
            return underlying.address;
        }

        var serializedSize = serializedSize();
        var newBuffer = LogUtil.allocatePage(serializedSize);
        var buffer = newBuffer.slice(ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET,
                        newBuffer.limit() - ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET).
                order(ByteOrder.nativeOrder());
        serializedSize -= ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET;

        assert buffer.alignmentOffset(ImmutableBasePage.KEY_PREFIX_LEN_OFFSET, Integer.BYTES) == 0;
        buffer.putInt(ImmutableBasePage.KEY_PREFIX_LEN_OFFSET, 0);

        assert buffer.alignmentOffset(ImmutableBasePage.ENTRIES_COUNT_OFFSET, Integer.BYTES) == 0;
        buffer.putInt(ImmutableBasePage.ENTRIES_COUNT_OFFSET, changedEntries.size());

        int keysPositionsOffset = ImmutableBasePage.KEYS_OFFSET;
        int keysDataOffset = ImmutableBasePage.KEYS_OFFSET + changedEntries.size() * 2 * Long.BYTES;

        int valuesPositionsOffset = ImmutableBasePage.KEYS_OFFSET + Long.BYTES * changedEntries.size();
        int valuesDataOffset = serializedSize;

        for (var entry : changedEntries) {
            var key = entry.key;
            var value = entry.value;

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

        byte type;
        if (parent == null) {
            type = ImmutableBTree.LEAF_ROOT_PAGE;
        } else {
            type = ImmutableBTree.LEAF_PAGE;
        }

        return log.writeInsideSinglePage(type, structureId, newBuffer, true);
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
                    final int childIndex = parent.find(firstKey);
                    assert childIndex >= 0;

                    parent.delete(childIndex);
                    return null;
                }

                assert parent.getEntriesCount() > 1 : "parent must have at least 2 children";

                // Destination node is right sibling if idx == 0, otherwise left sibling.
                // If both this node and the target node are too small then merge them.
                var parentIndex = parent.find(firstKey);
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
            size += entry.key.limit();
            size += entry.value.limit();
        }

        return size;
    }

    @Override
    public void spill() {
        if (spilled || changedEntries == null) {
            return;
        }

        var page = this;

        while (true) {
            var nextSiblingEntries = page.splitAtPageSize();

            if (nextSiblingEntries == null) {
                break;
            }

            if (parent == null) {
                parent = new MutableInternalPage(tree, null, expiredLoggables,
                        log, pageSize,
                        null);
                assert tree.root == this;

                tree.root = parent;

                if (firstKey == null) {
                    firstKey = changedEntries.get(0).key;
                }

                parent.addChild(firstKey, this);
            }

            page = new MutableLeafPage(tree, null, log, pageSize,
                    expiredLoggables, parent);

            page.changedEntries = nextSiblingEntries;
            page.firstKey = nextSiblingEntries.get(0).key;

            parent.addChild(page.firstKey, page);
            parent.sortBeforeInternalSpill = true;
        }

        spilled = true;

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
                ImmutableLeafPage.KEYS_OFFSET + 2 * Long.BYTES + firstEntry.key.limit()
                + firstEntry.value.limit();

        int indexToSplit = 0;


        for (int i = 1; i < changedEntries.size(); i++) {
            var entry = changedEntries.get(i);

            size += 2 * Long.BYTES + entry.key.limit() + entry.value.limit();
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

    public boolean fetch() {
        //already fetched
        if (underlying == null) {
            return false;
        }

        expiredLoggables.add(pageAddress, pageSize);

        final int size = underlying.getEntriesCount();
        changedEntries = new ObjectArrayList<>(size);

        for (int i = 0; i < size; i++) {
            var key = underlying.key(i);
            var value = underlying.value(i);

            changedEntries.add(new Entry(key, value));
        }

        firstKey = changedEntries.get(0).key;

        //do not keep copy of the data for a long time
        underlying = null;

        return true;
    }

    @Override
    public long treeSize() {
        return keyView.size();
    }

    @Override
    public long address() {
        if (underlying != null) {
            return underlying.address;
        }

        return Loggable.NULL_ADDRESS;
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


