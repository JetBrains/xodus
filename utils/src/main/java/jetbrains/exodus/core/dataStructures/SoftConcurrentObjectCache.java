package jetbrains.exodus.core.dataStructures;

public class SoftConcurrentObjectCache<K, V> extends SoftObjectCacheBase<K, V> {

    public SoftConcurrentObjectCache(final int cacheSize) {
        super(cacheSize);
    }

    @Override
    public void lock() {
    }

    @Override
    public void unlock() {
    }

    @Override
    protected ObjectCacheBase<K, V> newChunk(final int chunkSize) {
        return new ConcurrentObjectCache<K, V>(chunkSize);
    }
}