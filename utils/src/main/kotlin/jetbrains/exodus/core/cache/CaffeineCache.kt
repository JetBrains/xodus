package jetbrains.exodus.core.cache

import com.github.benmanes.caffeine.cache.Cache
import jetbrains.exodus.core.dataStructures.cache.BasicCache
import java.util.function.BiConsumer

class CaffeineCache<K, V>(
    private val config: CaffeineCacheConfig<K, V>,
    private val cache: Cache<K, V>
) : BasicCache<K, V> {

    companion object {

        fun <K, V> create(size: Int): CaffeineCache<K, V> {
            val config = CaffeineCacheConfig<K, V>(
                sizeEviction = FixedSizeEviction(maxSize = size.toLong())
            )
            return create(config)
        }

        fun <K, V> create(config: CaffeineCacheConfig<K, V>): CaffeineCache<K, V> {
            val cache: Cache<K, V> = CaffeineCacheBuilder.build(config)
            return CaffeineCache(config, cache)
        }
    }

    override fun size(): Long {
        return config.sizeEviction.size
    }

    override fun count(): Long {
        return cache.estimatedSize()
    }

    override fun get(key: K): V? {
        return cache.getIfPresent(key)
    }

    override fun put(key: K, v: V) {
        cache.put(key, v)
    }

    override fun remove(key: K) {
        cache.invalidate(key)
    }

    override fun clear() {
        cache.invalidateAll()
    }

    override fun forceEviction() {
        cache.cleanUp()
    }

    override fun forEachEntry(consumer: BiConsumer<K, V>) {
        cache.asMap().forEach(consumer)
    }
}