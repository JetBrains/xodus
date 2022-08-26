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

final class MutableInternalPage extends MutableBasePage<ImmutableInternalPage, MutableInternalPage.Entry> {
    final long pageAddress;
    long cachedTreeSize = -1;
    boolean unbalanced;

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
            changedEntries = new ObjectArrayList<>();
        }
    }

    @Override
    MutableBasePage<ImmutableInternalPage, Entry> newPage() {
        return new MutableInternalPage(tree, null, expiredLoggables, log);
    }


    MutablePage mutableChild(int index) {
        fetch();

        assert changedEntries != null;

        return changedEntries.get(index).mutablePage;
    }


    @Override
    public int getEntriesCount() {
        return keyView.size();
    }

    @Override
    public TraversablePage child(int index) {
        if (changedEntries == null) {
            assert underlying != null;
            return underlying.child(index);
        }

        return changedEntries.get(index).mutablePage;
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
        if (changedEntries == null) {
            assert underlying != null;
            return underlying.address;
        }

        assert serializedSize == serializedSize();
        var newBuffer = LogUtil.allocatePage(serializedSize);

        assert changedEntries.size() >= 2;
        assert newBuffer.limit() <= pageSize || changedEntries.size() < 4;

        byte type;
        if (parent == null) {
            type = ImmutableBTree.INTERNAL_ROOT_PAGE;
        } else {
            type = ImmutableBTree.INTERNAL_PAGE;
        }

        for (var entry : changedEntries) {
            var child = entry.mutablePage;
            entry.savedAddress = child.save(structureId, this);
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
        assert changedEntries != null;

        buffer.order(ByteOrder.nativeOrder());

        //we add Long.BYTES to preserver (sub)tree size
        assert buffer.alignmentOffset(ImmutableBasePage.KEY_PREFIX_LEN_OFFSET + Long.BYTES, Integer.BYTES) == 0;
        buffer.putInt(ImmutableBasePage.KEY_PREFIX_LEN_OFFSET + Long.BYTES, keyPrefixSize);

        assert buffer.alignmentOffset(ImmutableBasePage.ENTRIES_COUNT_OFFSET + Long.BYTES, Integer.BYTES) == 0;
        buffer.putInt(ImmutableBasePage.ENTRIES_COUNT_OFFSET + Long.BYTES, changedEntries.size());

        int keyPositionsOffset = ImmutableBasePage.KEYS_OFFSET + Long.BYTES;
        int childAddressesOffset = keyPositionsOffset + Long.BYTES * changedEntries.size();
        int keysDataOffset = childAddressesOffset + Long.BYTES * changedEntries.size();


        int treeSize = 0;
        for (var entry : changedEntries) {
            var key = entry.key;
            var keySize = key.limit();
            var child = entry.mutablePage;

            long subTreeSize = child.treeSize();
            treeSize += subTreeSize;

            assert buffer.alignmentOffset(keyPositionsOffset, Integer.BYTES) == 0;
            assert buffer.alignmentOffset(keyPositionsOffset + Integer.BYTES, Integer.BYTES) == 0;

            buffer.putInt(keyPositionsOffset, keysDataOffset - Long.BYTES);
            buffer.putInt(keyPositionsOffset + Integer.BYTES, keySize);

            assert buffer.alignmentOffset(childAddressesOffset, Long.BYTES) == 0;
            buffer.putLong(childAddressesOffset, entry.savedAddress);

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

        assert changedEntries != null;
        for (int i = 0; i < changedEntries.size(); i++) {
            var entry = changedEntries.get(i);
            var page = entry.mutablePage;

            var unbalanced = page.rebalance(this);

            if (unbalanced) {
                if (changedEntries.size() == 1) {
                    break;
                }

                if (i == 0) {
                    var nextEntry = changedEntries.remove(i + 1);
                    var nextPage = nextEntry.mutablePage;

                    nextPage.rebalance(this);

                    nextPage.fetch();
                    page.merge(nextPage);

                    serializedSize -= entrySize(nextEntry.key);
                } else {
                    var prevEntry = changedEntries.get(i - 1);
                    var prevPage = prevEntry.mutablePage;

                    prevPage.merge(page);

                    var removedEntry = changedEntries.remove(i);
                    serializedSize -= entrySize(removedEntry.key);

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
        unbalanced = changedEntries.size() < 2 || serializedSize < pageSize / 4;

        return unbalanced;
    }

    @Override
    public void unbalance() {
        unbalanced = true;
    }

    private int serializedSize() {
        assert changedEntries != null;

        int size = ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET + ImmutableLeafPage.KEYS_OFFSET +
                2 * Long.BYTES * changedEntries.size() + Long.BYTES;

        for (Entry entry : changedEntries) {
            size += entry.key.limit();
        }

        return size;
    }

    private int entrySize(ByteBuffer key) {
        return 2 * Long.BYTES + key.limit();
    }


    @Override
    public void merge(MutablePage page) {
        fetch();

        assert changedEntries != null;

        var internalPage = (MutableInternalPage) page;

        var internalChangedEntries = internalPage.changedEntries;
        assert internalChangedEntries != null;

        changedEntries.addAll(internalChangedEntries);

        serializedSize += internalChangedEntries.size() * 2 * Long.BYTES;
        for (var entry : internalChangedEntries) {
            serializedSize += entry.key.limit();
        }

        unbalanced = true;
    }

    public void updateFirstKey(ByteBuffer key) {
        fetch();

        assert this.changedEntries != null && !this.changedEntries.isEmpty();
        var entry = this.changedEntries.get(0);

        serializedSize -= entry.key.limit();
        entry.key = key;
        serializedSize += key.limit();
    }

    @Override
    public boolean spill(@Nullable MutableInternalPage parent, @NotNull ByteBuffer insertedKey,
                         @Nullable ByteBuffer parentUpperbound) {
        var spilled = doSpill(parent, insertedKey, parentUpperbound);

        assert changedEntries != null && changedEntries.size() <= 2 || serializedSize() <= pageSize;
        assert serializedSize == serializedSize();

        return spilled;
    }

    ObjectArrayList<Entry> splitAtPageSize() {
        assert changedEntries != null;

        //each page should contain at least two entries, root page can contain less entries
        if (changedEntries.size() < 4) {
            if (serializedSize < 0) {
                serializedSize = serializedSize();
            }
            return null;
        }

        var firstEntry = changedEntries.get(0);
        var secondEntry = changedEntries.get(1);


        int size = ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET +
                ImmutableInternalPage.KEYS_OFFSET + 2 * 2 * Long.BYTES + Long.BYTES;

        size += firstEntry.key.limit();
        size += secondEntry.key.limit();

        int indexSplitAt = 1;
        int currentSize = size;
        final int threshold = pageSize / 2;

        for (int i = 2; i < changedEntries.size(); i++) {
            var entry = changedEntries.get(i);
            size += entrySize(entry.key);

            if (size > threshold) {
                serializedSize = -1;
                break;
            }

            indexSplitAt = i;
            currentSize = size;
        }

        var splitResultSize = changedEntries.size() - (indexSplitAt + 1);
        if (splitResultSize == 1) {
            currentSize -= entrySize(changedEntries.get(indexSplitAt).key);
            indexSplitAt = indexSplitAt - 1;
        }

        ObjectArrayList<Entry> result = null;

        if (indexSplitAt < changedEntries.size() - 1) {
            result = new ObjectArrayList<>();
            result.addAll(0, changedEntries.subList(indexSplitAt + 1, changedEntries.size()));

            changedEntries.removeElements(indexSplitAt + 1, changedEntries.size());
        }

        if (serializedSize == -1) {
            serializedSize = currentSize;
        }

        return result;
    }

    void addChild(int index, ByteBuffer key, MutablePage page) {
        fetch();

        assert changedEntries != null;

        var entry = new Entry(key, page);

        changedEntries.add(index, entry);

        serializedSize += entrySize(key);
    }

    void updatePrefixSize(int from, int till, @Nullable ByteBuffer upperBound) {
        Entry secondEntry = null;

        assert changedEntries != null;

        var entriesSize = changedEntries.size();
        for (int i = from; i < till; i++) {
            Entry firstEntry;

            if (secondEntry == null) {
                firstEntry = changedEntries.get(i);
            } else {
                firstEntry = secondEntry;
            }


            ByteBuffer secondKey;
            if (i < entriesSize - 1) {
                secondEntry = changedEntries.get(i + 1);
                secondKey = secondEntry.key;
            } else {
                if (upperBound == null) {
                    break;
                } else {
                    secondKey = upperBound;
                }
            }


            var firstKey = firstEntry.key;

            int nextCalculatedPrefixSize;
            if (secondKey.remaining() == 0) {
                nextCalculatedPrefixSize = keyPrefixSize;
            } else {
                var partialKeyPrefixSize = MutableBTree.commonPrefix(firstKey, secondKey);
                nextCalculatedPrefixSize = keyPrefixSize + partialKeyPrefixSize;
            }

            var firstPage = firstEntry.mutablePage;

            var firstPageKeyPrefixSize = firstPage.getKeyPrefixSize();
            assert nextCalculatedPrefixSize >= firstPageKeyPrefixSize;

            if (nextCalculatedPrefixSize > firstPageKeyPrefixSize) {
                firstPage.truncateKeys(nextCalculatedPrefixSize - firstPageKeyPrefixSize);
            }
        }
    }

    public boolean fetch() {
        if (underlying == null) {
            return false;
        }

        expiredLoggables.add(pageAddress, pageSize);

        final int size = underlying.getEntriesCount();
        changedEntries = new ObjectArrayList<>(size);

        for (int i = 0; i < size; i++) {
            var key = underlying.key(i);
            var child = underlying.child(i);

            changedEntries.add(new Entry(key, child.toMutable(tree, expiredLoggables)));
        }

        underlying = null;
        return true;
    }

    @Override
    public long treeSize() {
        if (underlying != null) {
            return underlying.getTreeSize();
        }

        assert changedEntries != null;

        if (cachedTreeSize >= 0) {
            return cachedTreeSize;
        }

        int treeSize = 0;
        for (var entry : changedEntries) {
            treeSize += entry.mutablePage.treeSize();
        }

        return treeSize;
    }


    static final class Entry implements Comparable<Entry>, MutablePageEntry {
        ByteBuffer key;
        MutablePage mutablePage;

        long savedAddress;

        public Entry(ByteBuffer key, MutablePage mutablePage) {
            assert key != null;

            this.key = key;
            this.mutablePage = mutablePage;
        }

        @Override
        public int compareTo(@NotNull MutableInternalPage.Entry entry) {
            return ByteBufferComparator.INSTANCE.compare(key, entry.key);
        }

        @Override
        public ByteBuffer getKey() {
            return key;
        }

        @Override
        public void setKey(ByteBuffer key) {
            this.key = key;
        }
    }
}