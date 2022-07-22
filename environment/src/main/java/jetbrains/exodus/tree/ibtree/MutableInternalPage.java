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
    final int maxKeySize;

    ByteBufferHolder firstKey;

    final long pageAddress;

    boolean unbalanced;

    long cachedTreeSize = -1;

    MutableInternalPage(@Nullable ImmutableInternalPage underlying,
                        @NotNull ExpiredLoggableCollection expiredLoggables, @NotNull Log log,
                        int pageSize, @Nullable MutableInternalPage parent) {
        if (underlying != null) {
            pageAddress = underlying.address;
        } else {
            pageAddress = -1;
        }

        this.expiredLoggables = expiredLoggables;
        this.log = log;
        this.pageSize = pageSize;
        this.parent = parent;
        this.underlying = underlying;
        this.maxKeySize = pageSize / 4;

        if (underlying == null) {
            changedEntries = new ObjectArrayList<>();
        }

        keyView = new KeyView();
    }

    @Override
    public ByteBuffer key(int index) {
        return keyView.get(index);
    }

    MutablePage mutableChild(int index) {
        fetch();

        assert changedEntries != null;

        return changedEntries.get(index).mutablePage;
    }

    MutablePage mutableChildIfExists(int index) {
        if (changedEntries == null) {
            return null;
        }

        return changedEntries.get(index).mutablePage;
    }

    void delete(int index) {
        fetch();

        assert changedEntries != null;

        var entry = changedEntries.remove(index);
        entry.key.addExpiredLoggable(expiredLoggables);

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
            return underlying.address;
        }

        var newBuffer = LogUtil.allocatePage(serializedSize());
        var buffer = newBuffer.slice(ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET,
                        newBuffer.limit() - ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET).
                order(ByteOrder.nativeOrder());

        assert buffer.alignmentOffset(ImmutableBasePage.KEY_PREFIX_LEN_OFFSET + Long.BYTES, Integer.BYTES) == 0;
        buffer.putInt(ImmutableBasePage.KEY_PREFIX_LEN_OFFSET, 0);

        assert buffer.alignmentOffset(ImmutableBasePage.ENTRIES_COUNT_OFFSET + Long.BYTES, Integer.BYTES) == 0;
        buffer.putInt(ImmutableBasePage.ENTRIES_COUNT_OFFSET, changedEntries.size());

        int keyPositionsOffset = ImmutableBasePage.KEYS_OFFSET + Long.BYTES;
        int childAddressesOffset = keyPositionsOffset + Long.BYTES * changedEntries.size();
        int subTreeSizeOffset = childAddressesOffset + Long.BYTES * changedEntries.size();
        int embeddedKeysOffset = subTreeSizeOffset + Integer.BYTES * changedEntries.size();

        int treeSize = 0;
        for (var entry : changedEntries) {
            var child = entry.mutablePage;
            //we need to save mutableChild first to cache tree size, otherwise it could impact big performance
            //overhead during save of the data
            var childAddress = child.save(structureId);

            long subTreeSize = child.treeSize();
            treeSize += subTreeSize;

            var key = entry.key;
            var keyAddress = key.getAddress();

            if (keyAddress == 0) {
                var keyBuffer = key.getByteBuffer();
                if (keyBuffer.limit() <= maxKeySize) {
                    keyAddress = ((long) keyBuffer.limit() << Integer.SIZE) | embeddedKeysOffset;
                    buffer.put(embeddedKeysOffset, keyBuffer, 0, keyBuffer.limit());
                    embeddedKeysOffset += keyBuffer.limit();
                } else {
                    keyAddress = -log.write(ImmutableBTree.VALUE_NODE, structureId, new ByteBufferByteIterable(keyBuffer));
                }
            }

            assert buffer.alignmentOffset(keyPositionsOffset, Long.BYTES) == 0;
            buffer.putLong(keyPositionsOffset, keyAddress);
            keyPositionsOffset += Long.BYTES;

            assert buffer.alignmentOffset(childAddressesOffset, Long.BYTES) == 0;
            buffer.putLong(childAddressesOffset, childAddress);
            childAddressesOffset += Long.BYTES;

            assert buffer.alignmentOffset(subTreeSizeOffset, Integer.BYTES) == 0;
            buffer.putInt(subTreeSizeOffset, (int) subTreeSize);
            subTreeSizeOffset += Integer.BYTES;
        }

        assert buffer.alignmentOffset(0, Long.BYTES) == 0;
        buffer.putLong(0, treeSize);

        cachedTreeSize = treeSize;

        byte type;
        if (parent == null) {
            type = ImmutableBTree.INTERNAL_ROOT_PAGE;
        } else {
            type = ImmutableBTree.INTERNAL_PAGE;
        }

        return log.writeInsideSinglePage(type, structureId,
                newBuffer, true);
    }

    @Override
    public MutablePage rebalance() {
        if (changedEntries == null) {
            return null;
        }

        //re-balance children first, so if tree completely empty it will be collapsed
        for (var entry : changedEntries) {
            var newRoot = entry.mutablePage.rebalance();
            //only root can promote another root
            assert newRoot == null;
        }

        if (!unbalanced) {
            return null;
        }

        assert changedEntries != null;

        var threshold = pageSize / 4;
        // Ignore if node is above threshold (25%) and has enough keys.
        if (threshold < serializedSize() || changedEntries.size() < 2) {
            // Root node has special handling.
            if (parent == null) {
                // If root node only has one node then collapse it.
                if (changedEntries.size() == 1) {
                    //page is already marked as expired by call of the fetch method
                    //so we merely need to inform the tree that root is changed
                    return changedEntries.get(0).mutablePage;
                }

                return null;
            }

            // If node has no keys then just remove it.
            if (changedEntries.isEmpty()) {
                int index = parent.find(firstKey.getByteBuffer());
                parent.delete(index);
                return null;
            }

            assert parent.numChildren() > 1 : "parent must have at least 2 children";

            // Destination node is right sibling if idx == 0, otherwise left sibling.
            // If both this node and the target node are too small then merge them.
            var parentIndex = parent.find(firstKey.getByteBuffer());
            if (parentIndex == 0) {
                var nextSibling = (MutableInternalPage) parent.mutableChild(1);
                nextSibling.fetch();

                changedEntries.addAll(nextSibling.changedEntries);
                parent.delete(1);
            } else {
                var prevSibling = (MutableInternalPage) parent.mutableChild(parentIndex - 1);
                prevSibling.fetch();

                assert prevSibling.changedEntries != null;

                prevSibling.changedEntries.addAll(changedEntries);
                parent.delete(parentIndex);
            }
        }

        return null;
    }

    private int serializedSize() {
        assert changedEntries != null;

        int size = ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET + ImmutableLeafPage.KEYS_OFFSET +
                (2 * Long.BYTES + Integer.BYTES) * changedEntries.size();

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
                        log, pageSize,
                        null);
            }

            page = new MutableInternalPage(null, expiredLoggables, log, pageSize,
                    parent);
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

        //page should contain at least two entries, root page can contain less entries
        if (changedEntries.size() <= 2) {
            return null;
        }

        var firstEntry = changedEntries.get(0);
        var secondEntry = changedEntries.get(1);


        int size = ImmutableBTree.LOGGABLE_TYPE_STRUCTURE_METADATA_OFFSET +
                ImmutableInternalPage.KEYS_OFFSET + 2 * (2 * Long.BYTES + Integer.BYTES);

        size += firstEntry.key.embeddedSize();
        size += secondEntry.key.embeddedSize();

        int indexSplitAt = 1;

        for (int i = 2; i < changedEntries.size(); i++) {
            var entry = changedEntries.get(0);
            size += 2 * Long.BYTES + Integer.BYTES + entry.key.embeddedSize();

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

    void addChild(ByteBufferHolder key, MutablePage page) {
        fetch();

        assert changedEntries != null;

        changedEntries.add(new Entry(key, page));
    }

    private void fetch() {
        if (underlying == null) {
            return;
        }

        expiredLoggables.add(pageAddress, pageSize);

        final int size = underlying.getEntriesCount();
        changedEntries = new ObjectArrayList<>(size);

        for (int i = 0; i < size; i++) {
            var keyAddress = underlying.getKeyAddress(i);

            ByteBufferHolder keyHolder;
            if (keyAddress > 0) {
                var keyBuffer = underlying.getEmbeddedKey(keyAddress);
                keyHolder = new LoadedByteBufferHolder(keyBuffer, maxKeySize);
            } else {
                keyHolder = new LazyByteBufferHolder(log, -keyAddress);
            }

            var childAddress = underlying.getChildAddress(i);

            var childLoggable = log.readLoggableAsPage(childAddress);
            var childPage = childLoggable.getBuffer();
            var type = childLoggable.getType();

            final MutablePage child;
            if (type == ImmutableBTree.INTERNAL_PAGE) {
                var immutableChild = new ImmutableInternalPage(log, childPage, childAddress);
                child = new MutableInternalPage(immutableChild, expiredLoggables, log, pageSize, this);
            } else if (type == ImmutableBTree.LEAF_PAGE) {
                var immutableChild = new ImmutableLeafPage(log, childPage, childAddress);
                child = new MutableLeafPage(immutableChild, log, pageSize, expiredLoggables, this);
            } else {
                throw new IllegalStateException(String.format("Unexpected type of loggable %d.", type));
            }

            changedEntries.add(new Entry(keyHolder, child));
        }
    }

    int numChildren() {
        return keyView.size();
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

    static final class Entry implements Comparable<Entry> {
        ByteBufferHolder key;
        MutablePage mutablePage;

        public Entry(ByteBufferHolder key, MutablePage mutablePage) {
            this.key = key;
            this.mutablePage = mutablePage;
        }

        @Override
        public int compareTo(@NotNull MutableInternalPage.Entry entry) {
            return ByteBufferComparator.INSTANCE.compare(key.getByteBuffer(), entry.key.getByteBuffer());
        }
    }

    final class KeyView extends AbstractList<ByteBuffer> implements RandomAccess {
        @Override
        public ByteBuffer get(int index) {
            if (changedEntries != null) {
                return changedEntries.get(index).key.getByteBuffer();
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