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
package jetbrains.exodus.core.cache.persistent

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import java.util.concurrent.ConcurrentHashMap
import java.util.function.BiConsumer


typealias Version = Long

/**
 * This cache implementation is based on Caffeine cache. It versions each value stored.
 * Put or remove values for the current version doesn't affect other existing versions.
 */
class CaffeinePersistentCache<K, V> private constructor(
    private val cache: Cache<K, WeightedValueMap<Version, V>>,
    private val config: CaffeineCacheConfig<V>,
    override val version: Long,
    private val versionTracker: VersionTracker,
) : PersistentCache<K, V> {

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
                        weigher { _: K, values: WeightedValueMap<Version, V> -> values.totalWeight }
                    }
                }
                .build<K, WeightedValueMap<Version, V>>()
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
            val values = map ?: createNewValueMap()
            values.put(version, value)
            values.removeUnusedVersions(version)
            values
        }
        keyVersions[key] = version
    }

    private fun createNewValueMap(): WeightedValueMap<Version, V> {
        // Weigher is used only for weighted eviction
        val evictionConfig = config.sizeEviction as? WeightedEviction<V>
        return WeightedValueMap(evictionConfig?.weigher ?: { 1 })
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
        val nextVersion = versionTracker.nextVersion()
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

    override fun register(): PersistentCacheClient {
        val client = object : PersistentCacheClient {
            override fun unregister() {
                versionTracker.unregister(this, version)
            }
        }
        versionTracker.register(client, version)
        return client
    }

    private fun Cache<K, WeightedValueMap<Version, V>>.getVersioned(key: K, version: Version): V? {
        return this.getIfPresent(key)?.get(version)
    }

    // Returns true if values were changed
    private fun WeightedValueMap<Version, V>.removeUnusedVersions(currentVersion: Version): Boolean {
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

    private fun WeightedValueMap<Version, V>.removeUnusedVersionsAndPutBack(key: K, currentVersion: Version) {
        if (removeUnusedVersions(currentVersion)) {
            cache.put(key, this)
        }
    }
}