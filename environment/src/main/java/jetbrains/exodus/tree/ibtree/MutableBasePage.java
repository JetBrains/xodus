package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.ByteBufferComparator;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.tree.ExpiredLoggableCollection;
import jetbrains.exodus.util.ByteBuffers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Arrays;

abstract class MutableBasePage<U extends ImmutableBasePage> implements MutablePage {
    @Nullable
    U underlying;

    @Nullable
    ByteBuffer[] keys;

    int entriesSize;

    ByteBuffer keyPrefix;

    int serializedSize;

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
        this.log = log;
        this.pageSize = Math.min(4 * 1024, log.getCachePageSize());
        this.maxKeySize = pageSize / 4;

        this.expiredLoggables = expiredLoggables;
        this.underlying = underlying;

        this.tree = tree;
    }

    @Override
    public final int getKeyPrefixSize() {
        if (keyPrefix == null) {
            return 0;
        }

        return keyPrefix.limit();
    }

    @Override
    public ByteBuffer keyPrefix() {
        return keyPrefix;
    }

    @Override
    public final ByteBuffer key(int index) {
        if (underlying != null) {
            return underlying.key(index);
        }

        assert index < entriesSize;
        return keys[index];
    }


    @Override
    public final int find(ByteBuffer key) {
        if (underlying != null) {
            return underlying.find(key);
        }

        return Arrays.binarySearch(keys, 0, entriesSize, key, ByteBufferComparator.INSTANCE);
    }

    @Override
    public final void addKeyPrefix(ByteBuffer keyPrefixDiff) {
        fetch();

        assert keys != null;
        final int size = entriesSize;

        var keyPrefixDiffSize = keyPrefixDiff.remaining();
        var keyPrefixDiffPosition = keyPrefixDiff.position();

        for (int i = 0; i < size; i++) {
            assert keys[i] != null;

            var key = keys[i];

            var newKey = ByteBuffer.allocate(keyPrefixDiffSize + key.remaining());

            newKey.put(0, keyPrefixDiff, keyPrefixDiffPosition, keyPrefixDiffSize);
            newKey.put(keyPrefixDiffSize, key, key.position(), key.remaining());

            keys[i] = newKey;
        }

        keyPrefix = keyPrefix.slice(0, keyPrefix.limit() - keyPrefixDiffSize);
        serializedSize += (size - 1) * keyPrefixDiffSize;
    }

    @Override
    public final void truncateKeys(ByteBuffer keyPrefixDiff) {
        fetch();

        assert keys != null;
        final int size = entriesSize;

        final int keyPrefixDiffSize = keyPrefixDiff.remaining();
        for (int i = 0; i < size; i++) {
            var key = keys[i];

            assert key != null;
            var keySize = key.limit();

            keys[i] = key.slice(keyPrefixDiffSize, keySize - keyPrefixDiffSize);
        }

        serializedSize -= (size - 1) * keyPrefixDiffSize;

        var keyPrefixSize = getKeyPrefixSize();

        if (keyPrefixSize > 0) {
            var oldKeyPrefix = keyPrefix;
            keyPrefix = ByteBuffer.allocate(keyPrefixSize + keyPrefixDiffSize);

            keyPrefix.put(0, oldKeyPrefix, 0, keyPrefixSize);
            keyPrefix.put(keyPrefixSize, keyPrefixDiff, keyPrefixDiff.position(), keyPrefixDiff.limit());
        } else {
            keyPrefix = ByteBuffer.allocate(keyPrefixDiffSize);
            keyPrefix.put(0, keyPrefixDiff, 0, keyPrefixDiffSize);
        }
    }

    @Override
    public final long address() {
        if (underlying != null) {
            return underlying.address;
        }

        return Loggable.NULL_ADDRESS;
    }

    @Override
    public final int getEntriesCount() {
        if (underlying != null) {
            return underlying.getEntriesCount();
        }

        return entriesSize;
    }

    static ByteBuffer generateParentKey(@NotNull MutableInternalPage parent, @NotNull ByteBuffer insertedKey,
                                        int keyPrefixSize, @NotNull ByteBuffer key) {
        var parentKeyPrefix = parent.getKeyPrefixSize();
        var diff = keyPrefixSize - parentKeyPrefix;

        if (diff == 0) {
            return key;
        }

        return ByteBuffers.mergeBuffers(insertedKey.slice(parentKeyPrefix, diff), key);
    }
}