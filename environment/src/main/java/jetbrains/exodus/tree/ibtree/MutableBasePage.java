package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.CompoundByteIterable;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.tree.ExpiredLoggableCollection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

abstract class MutableBasePage<U extends ImmutableBasePage> implements MutablePage {
    @Nullable
    U underlying;

    @Nullable
    ByteIterable[] keys;

    int entriesSize;

    ByteIterable keyPrefix;

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

        return keyPrefix.getLength();
    }

    @Override
    public ByteIterable keyPrefix() {
        return keyPrefix;
    }

    @Override
    public final ByteIterable key(int index) {
        if (underlying != null) {
            return underlying.key(index);
        }

        assert index < entriesSize;
        return keys[index];
    }


    @Override
    public final int find(ByteIterable key) {
        if (underlying != null) {
            return underlying.find(key);
        }

        return Arrays.binarySearch(keys, 0, entriesSize, key);
    }

    @Override
    public final void addKeyPrefix(ByteIterable keyPrefixDiff) {
        fetch();

        assert keys != null;
        final int size = entriesSize;

        for (int i = 0; i < size; i++) {
            assert keys[i] != null;

            var key = keys[i];

            keys[i] = new CompoundByteIterable(keyPrefixDiff, key);
        }

        var keyPrefixDiffSize = keyPrefixDiff.getLength();

        keyPrefix = keyPrefix.subIterable(0, keyPrefix.getLength() - keyPrefixDiffSize);
        serializedSize += (size - 1) * keyPrefixDiffSize;
    }

    @Override
    public final void truncateKeys(ByteIterable keyPrefixDiff) {
        fetch();

        assert keys != null;
        final int size = entriesSize;

        final int keyPrefixDiffSize = keyPrefixDiff.getLength();
        for (int i = 0; i < size; i++) {
            var key = keys[i];

            assert key != null;
            var keySize = key.getLength();

            keys[i] = key.subIterable(keyPrefixDiffSize, keySize - keyPrefixDiffSize);
        }

        serializedSize -= (size - 1) * keyPrefixDiffSize;

        var keyPrefixSize = getKeyPrefixSize();

        if (keyPrefixSize > 0) {
            keyPrefix = new CompoundByteIterable(keyPrefix, keyPrefixDiff);
        } else {
            keyPrefix = keyPrefixDiff;
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
}