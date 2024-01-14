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
import com.github.benmanes.caffeine.cache.RemovalListener
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.function.BiConsumer
import kotlin.math.max


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
    private val latestKeyVersions: MutableMap<K, Long>
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
            val latestVersionKeys = ConcurrentHashMap<K, Long>()
            val evictionListener = RemovalListener<VersionedKey<K>, V> { versionedKey, _, _ ->
                if (versionedKey == null) {
                    return@RemovalListener
                }
                val (key, version) = versionedKey
                latestVersionKeys.compute(key) { _, latestVersion ->
                    if (version == latestVersion) null else latestVersion
                }
            }

            val cache = CaffeineCacheBuilder.build(config, VersionedKey<K>::key, evictionListener)
            val version = 0L
            val tracker = VersionTracker(version)

            return CaffeinePersistentCache(cache, config, version, tracker, latestVersionKeys)
        }
    }

    // Local index as map of keys with their corresponding versions available for the current version of cache
    private val keyVersions = ConcurrentHashMap<K, Long>()

    // Generic cache impl
    override fun size(): Long {
        return config.sizeEviction.size
    }

    override fun count(): Long {
        return cache.estimatedSize()
    }

    override fun get(key: K): V? {
        val entryVersion = keyVersions[key] ?: return null
        val versionedKey = VersionedKey(key, entryVersion)
        return cache.getIfPresent(versionedKey)
    }

    override fun put(key: K, value: V) {
        val currentVersion = version
        keyVersions.compute(key) { _, _ ->
            val versionedKey = VersionedKey(key, currentVersion)
            cache.put(versionedKey, value)

            currentVersion
        }
        latestKeyVersions.compute(key) { _, latestKeyVersion ->
            max(currentVersion, latestKeyVersion ?: -1)
        }
    }

    override fun remove(key: K) {
        keyVersions.computeIfPresent(key) { _, entryVersion ->
            val versionedKey = VersionedKey(key, entryVersion)
            cache.invalidate(versionedKey)
            null
        }
    }

    override fun clear() {
        cache.invalidateAll()
        keyVersions.clear()
        latestKeyVersions.clear()
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
        val newCache = CaffeinePersistentCache(cache, config, nextVersion, versionTracker, latestKeyVersions)

        // Copy keys available for the next cache
        // It effectively prohibits new version from seeing new values cached for previous versions
        keyVersions.forEach { (key, version) ->
            val versionedKey = VersionedKey(key, version)
            val value = cache.getIfPresent(versionedKey)
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
        if (leftClients == 0L && version < versionTracker.currentVersion) {
            keyVersions.forEach { (key, version) ->
                // Invalidate value in case if current version is not the latest one
                val latestVersion = latestKeyVersions[key]
                if (latestVersion == null || version <= latestVersion) {
                    cache.invalidate(VersionedKey(key, version))
                }
            }
        }
    }

    private fun Map<K, Long>.forEachCacheEntry(consumer: (K, V) -> Unit) {
        forEach { (key, version) ->
            val versionedKey = VersionedKey(key, version)
            val value = cache.getIfPresent(versionedKey)
            if (value != null) {
                consumer(key, value)
            }
        }
    }
}