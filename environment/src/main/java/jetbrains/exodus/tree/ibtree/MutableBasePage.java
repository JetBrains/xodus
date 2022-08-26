package jetbrains.exodus.tree.ibtree;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import jetbrains.exodus.ByteBufferComparator;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.tree.ExpiredLoggableCollection;
import jetbrains.exodus.util.ByteBuffers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Collections;
import java.util.RandomAccess;

abstract class MutableBasePage<U extends ImmutableBasePage, E extends MutablePageEntry> implements MutablePage {
    @Nullable
    U underlying;

    @Nullable
    ObjectArrayList<E> changedEntries;

    int keyPrefixSize;

    int serializedSize;

    @NotNull
    final KeyView keyView;

    @NotNull
    final ExpiredLoggableCollection expiredLoggables;

    @NotNull
    final Log log;

    final int pageSize;
    final int maxKeySize;

    @NotNull
    final MutableBTree tree;


    MutableBasePage(@NotNull final MutableBTree tree, @Nullable U underlying,
                    @NotNull ExpiredLoggableCollection expiredLoggables, @NotNull final Log log) {
        keyView = new KeyView();

        this.log = log;
        this.pageSize = log.getCachePageSize();
        this.maxKeySize = pageSize / 4;

        this.expiredLoggables = expiredLoggables;
        this.underlying = underlying;

        this.tree = tree;
    }

    @Override
    public final int getKeyPrefixSize() {
        return keyPrefixSize;
    }

    @Override
    public final ByteBuffer key(int index) {
        return keyView.get(index);
    }


    @Override
    public final int find(ByteBuffer key) {
        return Collections.binarySearch(keyView, key, ByteBufferComparator.INSTANCE);
    }

    @Override
    public final void addKeyPrefix(ByteBuffer prefix) {
        fetch();

        assert changedEntries != null;
        for (var entry : changedEntries) {
            var key = entry.getKey();
            entry.setKey(ByteBuffers.mergeBuffers(prefix, key));
        }

        var prefixSize = prefix.limit();

        keyPrefixSize -= prefixSize;
        serializedSize += changedEntries.size() * prefixSize;
    }

    @Override
    public final void truncateKeys(int keyPrefixSizeDiff) {
        fetch();

        assert changedEntries != null;
        for (var entry : changedEntries) {
            var key = entry.getKey();
            var keySize = key.limit();

            entry.setKey(key.slice(keyPrefixSizeDiff, keySize - keyPrefixSizeDiff));
        }

        serializedSize -= changedEntries.size() * keyPrefixSizeDiff;
        keyPrefixSize += keyPrefixSizeDiff;
    }

    abstract MutableBasePage<U, E> newPage();

    abstract ObjectArrayList<E> splitAtPageSize();

    @Override
    public final long address() {
        if (underlying != null) {
            return underlying.address;
        }

        return Loggable.NULL_ADDRESS;
    }

    final boolean doSpill(@Nullable MutableInternalPage parent, @NotNull ByteBuffer insertedKey,
                          @Nullable ByteBuffer parentUpperBound) {
        if (serializedSize <= pageSize || changedEntries == null) {
            return false;
        }

        var spilled = false;
        var page = this;

        int currentIndex;
        int addedChildren = 0;

        ByteBuffer parentPrefix;

        if (parent == null) {
            currentIndex = -1;
            parentPrefix = null;
        } else {
            var parentPrefixSize = parent.getKeyPrefixSize();
            assert parentPrefixSize <= keyPrefixSize;

            if (parentPrefixSize < keyPrefixSize) {
                parentPrefix = insertedKey.slice(parentPrefixSize, keyPrefixSize - parentPrefixSize);
            } else {
                parentPrefix = null;
            }

            currentIndex = parent.find(generateParentKey(parentPrefix, changedEntries.get(0).getKey()));

            if (currentIndex < 0) {
                currentIndex = -currentIndex - 2;
                assert currentIndex >= 0;
            }
        }


        while (true) {
            var nextSiblingEntries = page.splitAtPageSize();

            if (nextSiblingEntries == null) {
                break;
            }

            spilled = true;
            if (parent == null) {
                parent = new MutableInternalPage(tree, null, expiredLoggables, log);
                assert tree.root == this;
                tree.root = parent;


                parent.addChild(0, changedEntries.get(0).getKey(), page);
                currentIndex = 0;
            }

            page = newPage();
            page.changedEntries = nextSiblingEntries;
            //will be calculated at next call to splitAtPageSize()
            page.serializedSize = -1;
            page.keyPrefixSize = keyPrefixSize;

            addedChildren++;
            parent.addChild(currentIndex + addedChildren, generateParentKey(parentPrefix,
                    nextSiblingEntries.get(0).getKey()), page);
        }

        if (spilled) {
            parent.updatePrefixSize(currentIndex, currentIndex + addedChildren + 1, parentUpperBound);
        }

        return spilled;
    }

    private ByteBuffer generateParentKey(@Nullable ByteBuffer parentPrefix, @NotNull ByteBuffer key) {
        if (parentPrefix == null) {
            return key;
        }

        return ByteBuffers.mergeBuffers(parentPrefix, key);
    }

    final class KeyView extends AbstractList<ByteBuffer> implements RandomAccess {
        @Override
        public ByteBuffer get(int index) {
            if (changedEntries != null) {
                return changedEntries.get(index).getKey();
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