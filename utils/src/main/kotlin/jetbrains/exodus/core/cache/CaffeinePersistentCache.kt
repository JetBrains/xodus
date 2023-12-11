package jetbrains.exodus.core.dataStructures.cache

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Weigher
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong
import java.util.function.BiConsumer
import kotlin.collections.forEach


data class CaffeineCacheConfig(
    /**
     * Has no effect for weighted cache
     */
    val maxSize: Long = -1,
    val expireAfterAccess: Duration = Duration.ofMinutes(5),
    val useSoftValues: Boolean = true,
)

data class WeightCacheConfig<K, V>(
    val maxWeight: Long = -1,
    val weigher: (K, V) -> Int
)

/**
 * This cache implementation is based on Caffeine cache. It versions each value stored using versioned key.
 * It's allowed to put or remove values any version not affecting others.
 * Creating new copy is done via coping all values from the current version to the next one.
 *
 * Read-write lock is used for thread safety:
 * - Read lock - for **key level** operations like put(key) or remove(key) to allow them to execute concurrently.
 * - Write lock - for **global operations** like clear() to prevent it from executing concurrently with other operations.
 */
class CaffeinePersistentCache<K, V> private constructor(
    private val cache: Cache<VersionedKey<K>, V>,
    private val config: CaffeineCacheConfig,
    override val currentVersion: Long,
    private val versionTracker: VersionTracker,
) : PersistentCache<K, V> {

    data class VersionedKey<K>(val key: K, val version: Long)

    private class VersionTracker(initialVersion: Long = 0) {
        private val versionRef = AtomicLong(initialVersion)

        fun next(): Long {
            return versionRef.incrementAndGet()
        }
    }

    companion object {

        fun <K, V> createSized(config: CaffeineCacheConfig): CaffeinePersistentCache<K, V> {
            return create(config)
        }

        fun <K, V> createWeighted(
            config: CaffeineCacheConfig,
            weightConfig: WeightCacheConfig<K, V>
        ): CaffeinePersistentCache<K, V> {
            return create(config, weightConfig)
        }

        private fun <K, V> create(
            config: CaffeineCacheConfig,
            weightCacheConfig: WeightCacheConfig<K, V>? = null
        ): CaffeinePersistentCache<K, V> {
            val cache = Caffeine.newBuilder()
                .apply {
                    if (weightCacheConfig != null) {
                        maximumWeight(weightCacheConfig.maxWeight)
                        weigher(Weigher { key: VersionedKey<K>, value: V ->
                            weightCacheConfig.weigher(key.key, value)
                        })
                    } else {
                        maximumSize(config.maxSize)
                    }
                }
                .expireAfterAccess(config.expireAfterAccess)
                .apply { if (config.useSoftValues) softValues() }
                .build<VersionedKey<K>, V>()
            val version = 0L
            val tracker = VersionTracker(version)

            return CaffeinePersistentCache(cache, config, version, tracker)
        }
    }

    // Generic cache impl
    override fun size(): Int {
        return config.maxSize.toInt()
    }

    override fun count(): Int {
        return cache.estimatedSize().toInt()
    }

    override fun get(key: K): V? {
        val versionedKey = VersionedKey(key, currentVersion)
        return cache.getIfPresent(versionedKey)
    }

    override fun put(key: K, value: V) {
        val versionedKey = VersionedKey(key, currentVersion)
        cache.put(versionedKey, value)
    }

    override fun remove(key: K) {
        val versionedKey = VersionedKey(key, currentVersion)
        cache.invalidate(versionedKey)
    }

    override fun clear() {
        cache.invalidateAll()
    }

    override fun forEachEntry(consumer: BiConsumer<K, V>) {
        cache.asMap().entries.forEach { (key, value) ->
            consumer.accept(key.key, value)
        }
    }

    // Persistent cache impl
    override fun createNextVersion(entryConsumer: BiConsumer<K, V>?): PersistentCache<K, V> {
        val nextVersion = versionTracker.next()
        cache.asMap().forEach { (key, value) ->
            // Take value only from the current version
            if (value != null && key.version == currentVersion) {
                val versionedKey = VersionedKey(key.key, nextVersion)
                cache.put(versionedKey, value)
                entryConsumer?.accept(key.key, value)
            }
        }
        return CaffeinePersistentCache(cache, config, nextVersion, versionTracker)
    }
}