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
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.function.BiConsumer

typealias Version = Long
typealias ValueWeigher<V> = (V) -> Int

/**
 * This cache implementation is based on Caffeine cache. It versions each value stored.
 * Put or remove values for the current version doesn't affect other versions.
 */
class CaffeinePersistentCache<K, V> private constructor(
    private val cache: Cache<K, VersionedValues<V>>,
    private val config: CaffeineCacheConfig<V>,
    override val version: Long,
    private val versionTracker: VersionTracker,
) : PersistentCache<K, V> {

    // Thread-safe container for cached versioned values
    private class VersionedValues<V>(private val weigher: ValueWeigher<V>) {
        // Version -> Value map
        private val map = ConcurrentHashMap<Version, V>()

        // Total weight of all values collectively
        private val totalWeightRef = AtomicInteger()

        val totalWeight get() = totalWeightRef.get()
        val size get() = map.size
        val keys get() = map.keys

        fun put(version: Version, value: V) {
            map.compute(version) { _, prevValue ->
                val toSubtract = prevValue?.let(weigher) ?: 0
                totalWeightRef.updateAndGet { it + weigher(value) - toSubtract }
                value
            }
        }

        fun remove(version: Version) {
            map.computeIfPresent(version) { _, value ->
                totalWeightRef.updateAndGet { (it - weigher(value)).coerceAtLeast(0) }
                null
            }
        }

        fun get(version: Version): V? {
            return map[version]
        }

        fun orNullIfEmpty(): VersionedValues<V>? {
            return if (map.isEmpty()) null else this
        }
    }

    // Thread-safe class for tracking current version and clients registered for different versions
    private class VersionTracker(initialVersion: Long = 0) {

        data class ClientVersion(val client: CacheClient, val version: Long)

        private val registeredClients = ConcurrentHashMap.newKeySet<ClientVersion>()
        private val versionClientCount = ConcurrentHashMap<Long, Long>()
        private val versionRef = AtomicLong(initialVersion)

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
                versionClientCount[version]
            }
            return clientsLeft ?: 0
        }

        fun hasNoClients(version: Version): Boolean {
            return !versionClientCount.containsKey(version)
        }
    }

    companion object {

        fun <K, V> create(config: CaffeineCacheConfig<V>): CaffeinePersistentCache<K, V> {
            val cache = Caffeine.newBuilder()
                .apply { if (config.expireAfterAccess != null) expireAfterAccess(config.expireAfterAccess) }
                .apply { if (config.useSoftValues) softValues() }
                .apply { if (config.directExecution) executor(Runnable::run) }
                .apply {
                    if (config.sizeEviction is SizedEviction) {
                        maximumSize(config.sizeEviction.maxSize)
                    } else {
                        val eviction = config.sizeEviction as WeightedEviction
                        maximumWeight(eviction.maxWeight)
                        weigher { _: K, values: VersionedValues<V> -> values.totalWeight }
                    }
                }
                .build<K, VersionedValues<V>>()
            val version = 0L
            val tracker = VersionTracker(version)

            return CaffeinePersistentCache(cache, config, version, tracker)
        }
    }

    // Local index as map of keys to corresponding versions available for the current version of cache
    private val keyVersions = ConcurrentHashMap<K, Long>()

    // Generic cache impl
    override fun size(): Long {
        return cache.policy().eviction().orElseThrow().maximum
    }

    override fun count(): Long {
        return cache.estimatedSize()
    }

    override fun get(key: K): V? {
        val valueVersion = keyVersions[key] ?: return null
        val values = cache.getIfPresent(key) ?: return null
        values.removeUnusedVersionsAndPutBack(key, valueVersion)
        return values.get(valueVersion)
    }

    override fun put(key: K, value: V) {
        cache.asMap().compute(key) { _, map ->
            val values = map ?: VersionedValues((config.sizeEviction as? WeightedEviction<V>)?.weigher ?: { 1 })
            values.put(version, value)
            values.removeUnusedVersions(version)
            values
        }
        keyVersions[key] = version
    }

    override fun remove(key: K) {
        cache.asMap().computeIfPresent(key) { _, values ->
            values.remove(version)
            values.orNullIfEmpty()
        }
        keyVersions.remove(key)
    }

    override fun clear() {
        cache.invalidateAll()
        keyVersions.clear()
    }

    override fun forceEviction() {
        cache.cleanUp()
    }

    override fun forEachEntry(consumer: BiConsumer<K, V>) {
        keyVersions.forEach { (key, version) ->
            val value = cache.getVersioned(key, version)
            if (value != null) {
                consumer.accept(key, value)
            }
        }
    }

    // Persistent cache impl
    override fun createNextVersion(entryConsumer: BiConsumer<K, V>?): PersistentCache<K, V> {
        val nextVersion = versionTracker.next()
        val newCache = CaffeinePersistentCache(cache, config, nextVersion, versionTracker)

        // Copy key index for the next cache
        // It effectively prohibits new version from seeing new values cached for previous versions
        // as they might be stale, e.g. due to values already changed by another transaction
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
                versionTracker.unregister(this, version)
            }
        }
        versionTracker.register(client, version)
        return client
    }

    private fun Cache<K, VersionedValues<V>>.getVersioned(key: K, version: Version): V? {
        return this.getIfPresent(key)?.get(version)
    }

    // Returns true if values were changed
    private fun VersionedValues<V>.removeUnusedVersions(currentVersion: Version): Boolean {
        if (this.size <= 1) {
            return false
        }
        var changed = false
        for (version in this.keys) {
            if (version < currentVersion && versionTracker.hasNoClients(version)) {
                this.remove(version)
                changed = true
            }
        }
        return changed
    }

    private fun VersionedValues<V>.removeUnusedVersionsAndPutBack(key: K, currentVersion: Version) {
        if (removeUnusedVersions(currentVersion)) {
            cache.put(key, this)
        }
    }
}