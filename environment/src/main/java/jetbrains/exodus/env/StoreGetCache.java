package jetbrains.exodus.env;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.core.dataStructures.SoftConcurrentObjectCache;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Caches Store.get() results retrieved from immutable trees.
 * For each key and tree address (KeyEntry), value is immutable, so lock-free caching is ok.
 */
class StoreGetCache {

    private final SoftConcurrentObjectCache<KeyEntry, ByteIterable> cache;

    StoreGetCache(final int cacheSize) {
        cache = new SoftConcurrentObjectCache<KeyEntry, ByteIterable>(cacheSize);
    }

    @Nullable
    ByteIterable tryKey(final long treeRootAddress, @NotNull final ByteIterable key) {
        return cache.tryKey(new KeyEntry(treeRootAddress, key));
    }

    void cacheObject(final long treeRootAddress, @NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        cache.cacheObject(new KeyEntry(treeRootAddress, key), value);
    }

    private static class KeyEntry {

        private final long treeRootAddress;
        @NotNull
        private final ByteIterable key;

        KeyEntry(final long treeRootAddress, @NotNull final ByteIterable key) {
            this.treeRootAddress = treeRootAddress;
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final KeyEntry right = (KeyEntry) o;
            return treeRootAddress == right.treeRootAddress && key.equals(right.key);
        }

        @Override
        public int hashCode() {
            final int keyHashCode = key.hashCode();
            int result = (int) ((treeRootAddress + keyHashCode) ^ (treeRootAddress >>> 32));
            result = 31 * result + keyHashCode;
            return result;
        }
    }
}
