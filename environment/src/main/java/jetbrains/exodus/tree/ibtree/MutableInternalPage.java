package jetbrains.exodus.tree.ibtree;

import it.unimi.dsi.fastutil.longs.LongArrayList;
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

final class MutableInternalPage implements MutablePage {
    @Nullable
    MutableInternalPage parent;

    @Nullable
    ImmutableInternalPage underlying;

    @Nullable
    ObjectArrayList<Entry> changedEntries;

    /**
     * Children inform parent that it should sort {@link #changedEntries} before performing their spill
     */
    boolean sortBeforeInternalSpill;

    @NotNull
    final KeyView keyView;

    @NotNull
    final ExpiredLoggableCollection expiredLoggables;

    @NotNull
    final Log log;

    final int pageSize;
    final int pageOffset;

    final ByteBuffer firstKey;

    boolean unbalanced;


    MutableInternalPage(@Nullable ImmutableInternalPage underlying,
                        @NotNull ExpiredLoggableCollection expiredLoggables, @NotNull Log log,
                        int pageSize, int pageOffset,
                        @Nullable MutableInternalPage parent) {
        this.expiredLoggables = expiredLoggables;
        this.log = log;
        this.pageSize = pageSize;
        this.parent = parent;
        this.pageOffset = pageOffset;
        this.underlying = underlying;

        keyView = new KeyView();
        firstKey = keyView.get(0);
    }

    @Override
    public ByteBuffer key(int index) {
        return keyView.get(index);
    }

    MutablePage child(int index) {
        fetch();

        assert changedEntries != null;

        return changedEntries.get(index).mutablePage;
    }

    void delete(int index) {
        fetch();

        assert changedEntries != null;

        changedEntries.remove(index);
        unbalanced = true;
    }

    @Override
    public int find(ByteBuffer key) {
        return Collections.binarySearch(keyView, key, ByteBufferComparator.INSTANCE);
    }


    @Override
    public long save(int structureId) {
        if (changedEntries == null) {
            assert underlying != null;
            return underlying.pageIndex;
        }

        final LongArrayList childIndexes = new LongArrayList(changedEntries.size());
        for (var entry : changedEntries) {
            long childIndex = entry.mutablePage.save(structureId);
            childIndexes.add(childIndex);
        }

        var newBuffer = LogUtil.allocatePage(pageSize);
        var buffer = newBuffer.slice(pageOffset, pageSize - pageOffset).
                order(ByteOrder.nativeOrder());

        assert buffer.alignmentOffset(ImmutableBasePage.KEY_PREFIX_LEN_OFFSET, Integer.BYTES) == 0;
        buffer.putInt(ImmutableBasePage.KEY_PREFIX_LEN_OFFSET, 0);

        assert buffer.alignmentOffset(ImmutableBasePage.ENTRIES_COUNT_OFFSET, Integer.BYTES) == 0;
        buffer.putInt(ImmutableBasePage.ENTRIES_COUNT_OFFSET, changedEntries.size());

        int childIndexesOffset = ImmutableInternalPage.KEYS_OFFSET +
                ImmutableInternalPage.KEY_ENTRY_SIZE * changedEntries.size();

        for (int i = 0; i < childIndexes.size(); i++) {
            var childIndex = childIndexes.getLong(i);
            assert buffer.alignmentOffset(childIndexesOffset, Long.BYTES) == 0;

            buffer.putLong(childIndexesOffset, childIndex);

            childIndexesOffset += ImmutableInternalPage.CHILD_INDEX_SIZE;
        }

        int subTreeEntitiesCountIndexOffset = ImmutableInternalPage.KEYS_OFFSET +
                (ImmutableInternalPage.KEY_ENTRY_SIZE + ImmutableInternalPage.CHILD_INDEX_SIZE) *
                        changedEntries.size();
        for (Entry changedEntry : changedEntries) {
            var childCount = changedEntry.entriesCount;

            assert buffer.alignmentOffset(subTreeEntitiesCountIndexOffset, Integer.BYTES) == 0;
            buffer.putInt(subTreeEntitiesCountIndexOffset, childCount);
            subTreeEntitiesCountIndexOffset += ImmutableInternalPage.SUBTREE_ENTITIES_COUNT_SIZE;
        }

        int keyOffset = ImmutableInternalPage.KEYS_OFFSET +
                (ImmutableInternalPage.KEY_ENTRY_SIZE + ImmutableInternalPage.SUBTREE_ENTITIES_COUNT_SIZE +
                        ImmutableInternalPage.SUBTREE_ENTITIES_COUNT_SIZE) * changedEntries.size();

        int keyPositionSizeOffset = ImmutableInternalPage.KEYS_OFFSET;

        final int startKeyOffset = keyOffset;
        int size = pageOffset + keyOffset;

        for (var entry : changedEntries) {
            assert buffer.alignmentOffset(keyPositionSizeOffset, Integer.BYTES) == 0;
            buffer.putInt(keyOffset, keyOffset);
            keyPositionSizeOffset += ImmutableInternalPage.KEY_POSITION_SIZE;

            final int keySize = entry.key.limit();

            assert buffer.alignmentOffset(keyPositionSizeOffset, Integer.BYTES) == 0;
            buffer.putInt(keyPositionSizeOffset, keySize);

            keyPositionSizeOffset += ImmutableInternalPage.KEY_SIZE_SIZE;

            buffer.put(keyOffset, entry.key, 0, keySize);
            keyOffset += keySize;
        }

        size += (keyOffset - startKeyOffset);

        final int pages;
        if (size + pageOffset <= pageSize) {
            pages = 1;
        } else {
            pages = (((size - (pageSize - pageOffset)) + pageSize - 1) / pageSize) + 1;
        }

        assert buffer.alignmentOffset(ImmutableInternalPage.PAGES_COUNT_OFFSET, Short.BYTES) == 0;
        buffer.putShort(ImmutableInternalPage.PAGES_COUNT_OFFSET, (short) pages);

        return log.writeNewPage(newBuffer);
    }

    @Override
    public void rebalance() {

    }

    @Override
    public void spill() {
        if (changedEntries == null) {
            return;
        }

        //spill children first
        for (var childEntry : changedEntries) {
            childEntry.mutablePage.spill();
        }

        //new children were appended sort them
        if (sortBeforeInternalSpill) {
            changedEntries.sort(null);
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

            page = new MutableInternalPage(null, expiredLoggables, log, pageSize, 16,
                    parent);

            //recursively sum entries of sub-tree
            int totalEntries = changedEntries.size();

            for (var entry : changedEntries) {
                totalEntries += entry.entriesCount;
            }

            parent.addChild(page.firstKey, totalEntries, page);
            parent.sortBeforeInternalSpill = true;
        }

        //parent first spill children then itself
        //so we do not need sort children of parent or spill parent itself
    }

    private ObjectArrayList<Entry> splitAtPageSize() {
        assert changedEntries != null;

        //page should contain at least two entries, root page can contain less entries
        if (changedEntries.size() <= 2) {
            return null;
        }

        var firstEntry = changedEntries.get(0);
        var secondEntry = changedEntries.get(1);


        int size = pageOffset + ImmutableInternalPage.KEYS_OFFSET + 2 * (ImmutableInternalPage.KEY_ENTRY_SIZE +
                ImmutableInternalPage.CHILD_INDEX_SIZE + ImmutableInternalPage.SUBTREE_ENTITIES_COUNT_SIZE);

        size += firstEntry.key.limit();
        size += secondEntry.key.limit();

        int indexSplitAt = 1;

        for (int i = 2; i < changedEntries.size(); i++) {
            size += ImmutableInternalPage.KEY_ENTRY_SIZE +
                    ImmutableInternalPage.CHILD_INDEX_SIZE + ImmutableInternalPage.SUBTREE_ENTITIES_COUNT_SIZE;
            var entry = changedEntries.get(0);
            size += entry.key.limit();

            if (size > pageSize) {
                break;
            }

            indexSplitAt = i;
        }

        ObjectArrayList<Entry> result = null;

        if (indexSplitAt < changedEntries.size() - 1) {
            result = new ObjectArrayList<>();
            result.addAll(0, changedEntries.subList(indexSplitAt + 1, changedEntries.size()));

            changedEntries.removeElements(indexSplitAt + 1, changedEntries.size());
        }

        return result;
    }

    void addChild(ByteBuffer key, int entriesCount, MutablePage page) {
        fetch();

        assert changedEntries != null;

        changedEntries.add(new Entry(key, page, entriesCount));
    }

    private void fetch() {
        if (underlying == null) {
            return;
        }

        expiredLoggables.add(underlying.pageIndex * pageSize,
                underlying.getPagesCount() * pageSize);

        final int size = underlying.getEntriesCount();
        changedEntries = new ObjectArrayList<>(size);

        for (int i = 0; i < size; i++) {
            final ByteBuffer key = underlying.getKeyUnsafe(i);
            final long childIndex = underlying.getChildIndex(i);
            final int entriesCount = underlying.getSubTreeEntitiesCount(i);

            var child = new ImmutableInternalPage(log, pageSize, childIndex, 16);
            changedEntries.add(new Entry(key, new MutableInternalPage(child, expiredLoggables, log,
                    pageSize, 16, parent), entriesCount));

        }
    }

    int numChildren() {
        return keyView.size();
    }

    static final class Entry implements Comparable<Entry> {
        ByteBuffer key;
        MutablePage mutablePage;
        int entriesCount;

        public Entry(ByteBuffer key, MutablePage mutablePage, int entriesCount) {
            this.key = key;
            this.mutablePage = mutablePage;
            this.entriesCount = entriesCount;
        }

        @Override
        public int compareTo(@NotNull MutableInternalPage.Entry entry) {
            return ByteBufferComparator.INSTANCE.compare(key, entry.key);
        }
    }

    final class KeyView extends AbstractList<ByteBuffer> implements RandomAccess {
        @Override
        public ByteBuffer get(int index) {
            if (changedEntries != null) {
                return changedEntries.get(index).key;
            }

            assert underlying != null;

            return underlying.key(index);
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