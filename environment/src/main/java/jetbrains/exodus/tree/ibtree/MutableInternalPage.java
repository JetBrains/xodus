package jetbrains.exodus.tree.ibtree;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

final class MutableInternalPage implements MutablePage {
    MutableInternalPage parent;

    @Nullable
    ImmutableInternalPage underlying;
    ObjectArrayList<Entry> changedEntries;

    /**
     * Children inform parent that it should sort {@link #changedEntries} before pefroming their spill
     */
    boolean sortBeforeInternalSpill;

    @Override
    public ByteBuffer key(int index) {
        return null;
    }

    MutablePage child(int index) {
        return null;
    }

    void delete(int index) {
    }

    @Override
    public int find(ByteBuffer key) {
        return 0;
    }


    @Override
    public long save(int structureId) {
        return 0;
    }

    @Override
    public void rebalance() {
    }

    @Override
    public void spill() {
    }

    public int numChildren() {
        return -1;
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
}