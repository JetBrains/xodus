package jetbrains.exodus.core.dataStructures.cache

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Weigher
import jetbrains.exodus.core.cache.CaffeineCacheBuilder
import jetbrains.exodus.core.cache.CaffeineCacheConfig
import jetbrains.exodus.core.cache.FixedSizeEviction
import jetbrains.exodus.core.cache.WeightSizeEviction
import java.util.concurrent.ConcurrentHashMap
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

    private data class VersionedKey<K>(val key: K, val version: Long)

    private class VersionTracker(initialVersion: Long = 0) {

        data class ClientVersion(val client: CacheClient, val version: Long)

        private val registeredClients = ConcurrentHashMap.newKeySet<ClientVersion>()
        private val versionClientCount = ConcurrentHashMap<Long, Long>()
        private val versionRef = AtomicLong(initialVersion)
        val currentVersion get() = versionRef.get()

        fun next(): Long {
            return versionRef.incrementAndGet()
        }

        fun register(client: CacheClient, version: Long) {
            val wasAdded = registeredClients.add(ClientVersion(client, version))
            if (wasAdded) {
                versionClientCount.compute(version) { _, count -> if (count == null) 1 else count + 1 }
            }
        }

        fun unregister(client: CacheClient, version: Long): Long {
            val wasRemoved = registeredClients.remove(ClientVersion(client, version))
            val clientsLeft = if (wasRemoved) {
                versionClientCount.compute(version) { _, count ->
                    if (count == null || (count - 1) == 0L) {
                        null
                    } else {
                        count - 1
                    }
                }
            } else {
                versionClientCount.get(version)
            }
            return clientsLeft ?: 0
        }
    }

    companion object {

        fun <K, V> create(config: CaffeineCacheConfig<K, V>): CaffeinePersistentCache<K, V> {
            val cache = CaffeineCacheBuilder.build(config, VersionedKey<K>::key)
            val version = 0L
            val tracker = VersionTracker(version)

            return CaffeinePersistentCache(cache, config, version, tracker)
        }
    }

    private val currentVersionKeys = ConcurrentHashMap.newKeySet<K>()

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
        currentVersionKeys.add(key)
    }

    override fun remove(key: K) {
        val versionedKey = VersionedKey(key, version)
        cache.invalidate(versionedKey)
        currentVersionKeys.remove(key)
    }

    override fun clear() {
        cache.invalidateAll()
        currentVersionKeys.clear()
    }

    override fun forceEviction() {
        cache.cleanUp()
    }

    override fun forEachEntry(consumer: BiConsumer<K, V>) {
        currentVersionKeys.forEachCacheEntry(consumer::accept)
    }

    // Persistent cache impl
    override fun createNextVersion(entryConsumer: BiConsumer<K, V>?): PersistentCache<K, V> {
        val nextVersion = versionTracker.next()
        val newCache = CaffeinePersistentCache(cache, config, nextVersion, versionTracker)
        currentVersionKeys.forEachCacheEntry { key, value ->
            newCache.put(key, value)
            entryConsumer?.accept(key, value)
        }
        return newCache
    }

    override fun register(): CacheClient {
        val client = object : CacheClient {
            override fun unregister() {
                unregisterAndCleanUp(this)
            }
        }
        versionTracker.register(client, version)
        return client
    }

    private fun unregisterAndCleanUp(client: CacheClient) {
        val leftClients = versionTracker.unregister(client, version)
        if (leftClients == 0L && version < versionTracker.currentVersion) {
            currentVersionKeys.forEachVersionedKey {
                cache.invalidate(it)
            }
        }
    }

    private fun Set<K>.forEachVersionedKey(consumer: (VersionedKey<K>) -> Unit) {
        forEach { key ->
            val versionedKey = VersionedKey(key, version)
            consumer(versionedKey)
        }
    }

    private fun Set<K>.forEachCacheEntry(consumer: (K, V) -> Unit) {
        forEach { key ->
            val versionedKey = VersionedKey(key, version)
            val value = cache.getIfPresent(versionedKey)
            if (value != null) {
                consumer(key, value)
            }
        }
    }
}