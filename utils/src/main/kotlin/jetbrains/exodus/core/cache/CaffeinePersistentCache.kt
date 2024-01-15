/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.core.cache

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.function.BiConsumer

typealias Version = Long
typealias VersionedValueMap<V> = LinkedHashMap<Version, V>

/**
 * This cache implementation is based on Caffeine cache. It versions each value stored using versioned key.
 * It's allowed to put or remove values any version not affecting others.
 * Creating new copy is done via coping all values from the current version to the next one.
 */
class CaffeinePersistentCache<K, V> private constructor(
    private val cache: Cache<K, VersionedValueMap<V>>,
    private val config: CaffeineCacheConfig,
    override val version: Long,
    private val versionTracker: VersionTracker,
) : PersistentCache<K, V> {

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

        fun <K, V> create(config: CaffeineCacheConfig): CaffeinePersistentCache<K, V> {
            val cache = Caffeine.newBuilder()
                .withConfig(config)
                .run {
                    maximumWeight(config.maxSize)
                    weigher { _: K, map: VersionedValueMap<V> -> map.size }
                }
                .build<K, VersionedValueMap<V>>()
            val version = 0L
            val tracker = VersionTracker(version)

            return CaffeinePersistentCache(cache, config, version, tracker)
        }
    }

    // Local index as map of keys to corresponding versions available for the current version of cache
    private val keyVersions = ConcurrentHashMap<K, Long>()

    // Generic cache impl
    override fun size(): Long {
        return config.maxSize
    }

    override fun count(): Long {
        return cache.estimatedSize()
    }

    override fun get(key: K): V? {
        val entryVersion = keyVersions[key] ?: return null
        return cache.getVersioned(key, entryVersion)
    }

    override fun put(key: K, value: V) {
        cache.asMap().compute(key) { _, map ->
            val versionedValues = map ?: VersionedValueMap()
            versionedValues[version] = value
            keyVersions[key] = version
            versionedValues
        }
    }

    override fun remove(key: K) {
        cache.asMap().compute(key) { _, map ->
            map?.remove(version)
            keyVersions.remove(key)
            map.orNullIfEmpty()
        }
    }

    override fun clear() {
        cache.invalidateAll()
        keyVersions.clear()
    }

    override fun forceEviction() {
        cache.cleanUp()
    }

    override fun forEachEntry(consumer: BiConsumer<K, V>) {
        keyVersions.forEachCacheEntry(consumer::accept)
    }

    // Persistent cache impl
    override fun createNextVersion(entryConsumer: BiConsumer<K, V>?): PersistentCache<K, V> {
        val nextVersion = versionTracker.next()
        val newCache = CaffeinePersistentCache(cache, config, nextVersion, versionTracker)

        // Copy key index for the next cache
        // It effectively prohibits new version from seeing new values cached for previous versions
        keyVersions.forEach { (key, version) ->
            val value = cache.getVersioned(key, version)
            if (value != null) {
                newCache.keyVersions[key] = version
                entryConsumer?.accept(key, value)
            }
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
        if (leftClients > 0) {
            // There are clients left, thus don't do anything
            return
        }
        keyVersions.forEach { (key, _) ->
            cache.asMap().compute(key) { _, map ->
                // Remove only in case current version is not the latest one
                if (map?.keys?.lastOrNull() != version) {
                    map?.remove(version)
                }
               map.orNullIfEmpty()
            }
        }
    }

    private fun Map<K, Long>.forEachCacheEntry(consumer: (K, V) -> Unit) {
        forEach { (key, version) ->
            val value = cache.getVersioned(key, version)
            if (value != null) {
                consumer(key, value)
            }
        }
    }

    private fun Cache<K, VersionedValueMap<V>>.getVersioned(key: K, version: Version): V? {
        return this.getIfPresent(key)?.get(version)
    }

    private fun VersionedValueMap<V>?.orNullIfEmpty(): VersionedValueMap<V>? {
        return if (isNullOrEmpty()) null else this
    }
}