package jetbrains.exodus.core.dataStructures.cache

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Weigher
import jetbrains.exodus.core.cache.CaffeineCacheConfig
import jetbrains.exodus.core.cache.FixedSizeEviction
import jetbrains.exodus.core.cache.WeightSizeEviction
import java.util.concurrent.atomic.AtomicLong
import java.util.function.BiConsumer
import kotlin.collections.forEach


/**
 * This cache implementation is based on Caffeine cache. It versions each value stored using versioned key.
 * It's allowed to put or remove values any version not affecting others.
 * Creating new copy is done via coping all values from the current version to the next one.
 */
class CaffeinePersistentCache<K, V> private constructor(
    private val cache: Cache<VersionedKey<K>, V>,
    private val config: CaffeineCacheConfig<K, V>,
    override val version: Long,
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

        fun <K, V> create(config: CaffeineCacheConfig<K, V>): CaffeinePersistentCache<K, V> {
            val cache = Caffeine.newBuilder()
                // Size eviction
                .apply {
                    val sizeEviction = config.sizeEviction
                    when (sizeEviction) {
                        is FixedSizeEviction -> {
                            maximumSize(sizeEviction.maxSize)
                        }

                        is WeightSizeEviction -> {
                            maximumWeight(sizeEviction.maxWeight)
                            weigher(Weigher { key: VersionedKey<K>, value: V ->
                                sizeEviction.weigher(key.key, value)
                            })
                        }
                    }
                }
                // Time eviction
                .apply { if (config.expireAfterAccess != null) expireAfterAccess(config.expireAfterAccess) }
                // Reference eviction
                .apply { if (config.useSoftValues) softValues() }
                .apply { if (config.directExecution) executor(Runnable::run) }
                .build<VersionedKey<K>, V>()

            val version = 0L
            val tracker = VersionTracker(version)

            return CaffeinePersistentCache(cache, config, version, tracker)
        }
    }

    // Generic cache impl
    override fun size(): Long {
        return config.sizeEviction.size
    }

    override fun count(): Long {
        return cache.estimatedSize()
    }

    override fun get(key: K): V? {
        val versionedKey = VersionedKey(key, version)
        return cache.getIfPresent(versionedKey)
    }

    override fun put(key: K, value: V) {
        val versionedKey = VersionedKey(key, version)
        cache.put(versionedKey, value)
    }

    override fun remove(key: K) {
        val versionedKey = VersionedKey(key, version)
        cache.invalidate(versionedKey)
    }

    override fun clear() {
        cache.invalidateAll()
    }

    override fun forceEviction() {
        cache.cleanUp()
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
            if (value != null && key.version == version) {
                val versionedKey = VersionedKey(key.key, nextVersion)
                cache.put(versionedKey, value)
                entryConsumer?.accept(key.key, value)
            }
        }
        return CaffeinePersistentCache(cache, config, nextVersion, versionTracker)
    }
}