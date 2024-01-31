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
import jetbrains.exodus.core.dataStructures.persistent.PersistentHashMap
import java.lang.ref.Cleaner
import java.util.function.BiConsumer
import java.util.function.Consumer


typealias Version = Long

/**
 * This cache implementation is based on Caffeine cache. It versions each value stored.
 * Put or remove values for the current version doesn't affect other existing versions.
 */
class CaffeinePersistentCache<K : Any, V> private constructor(
    private val cache: Cache<K, WeightedValueMap<Version, V>>,
    private val config: CaffeineCacheConfig<V>,
    override val version: Long,
    private val versionTracker: VersionTracker,
    private val evictionSubject: CacheEvictionSubject<K>,
    // Local index as map of keys to corresponding versions available for the current version of cache
    // This map is eventually consistent with the cache and not intended to be in full sync with it due to performance reasons
    private val keyVersionIndex: PersistentHashMap<K, Version> = PersistentHashMap(),
    override val keyIndex: PersistentKeyIndex<K>? = null,
) : PersistentCache<K, V> {

    companion object {

        private val cleaner = Cleaner.create()

        fun <K : Any, V> create(config: CaffeineCacheConfig<V>, persistentKeyIndex: PersistentKeyIndex<K>? = null): CaffeinePersistentCache<K, V> {
            val evictionSubject = CacheEvictionSubject<K>()

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
                .removalListener(evictionSubject)
                .build<K, WeightedValueMap<Version, V>>()
            val version = 0L
            val tracker = VersionTracker(version)

            return CaffeinePersistentCache(cache, config, version, tracker, evictionSubject,  PersistentHashMap(), persistentKeyIndex)
        }
    }

    private val cacheMap = cache.asMap()

    // The implementation assumes that there is at least a single the most actual cache client is registered for eviction,
    // otherwise it might lead to memory leaks as key eviction is ignored and keys might retain in key index forever
    private val evictionListener: EvictionListener<K> = { key: K? -> key?.let { keyVersionIndex.removeKey(key); keyIndex?.remove(key) } }

    init {
        evictionSubject.addListener(evictionListener)
        cleaner.register(this) { evictionSubject.removeListener(evictionListener) }
    }

    // Generic cache impl
    private val _size by lazy { cache.policy().eviction().orElseThrow().maximum }
    override fun size(): Long {
        return _size
    }

    override fun count(): Long {
        return cache.estimatedSize()
    }

    override fun get(key: K): V? {
        val valueVersion = keyVersionIndex.current.get(key) ?: return null
        val values = cache.getIfPresent(key)
        if (values == null) {
            keyVersionIndex.removeKey(key)
            keyIndex?.remove(key)
            return null
        } else {
            values.removeStaleVersionsAndUpdateCache(key, valueVersion)
            return values.get(valueVersion)
        }
    }

    override fun put(key: K, value: V) {
        cacheMap.compute(key) { _, map ->
            val values = map ?: createNewValueMap()
            values.put(version, value)
            values.removeStaleVersions(version)
            values
        }
        keyVersionIndex.put(key, version)
        keyIndex?.put(key)
    }

    private fun createNewValueMap(): WeightedValueMap<Version, V> {
        // Weigher is used only for weighted eviction
        val evictionConfig = config.sizeEviction as? WeightedEviction<V>
        return WeightedValueMap(evictionConfig?.weigher ?: { 1 })
    }

    override fun remove(key: K) {
        cacheMap.computeIfPresent(key) { _, values ->
            values.remove(version)
            values.removeStaleVersions(version)
            values.orNullIfEmpty()
        }
        keyVersionIndex.removeKey(key)
        keyIndex?.remove(key)
    }

    override fun clear() {
        cache.invalidateAll()
    }

    override fun forceEviction() {
        cache.cleanUp()
    }

    override fun forEachEntry(consumer: BiConsumer<K, V>) {
        keyVersionIndex.current.forEach { entry ->
            val key = entry.key
            val version = entry.value
            val value = cache.getVersioned(key, version)
            if (value != null) {
                consumer.accept(key, value)
            }
        }
    }

    override fun forEachKey(consumer: Consumer<K>) {
        keyVersionIndex.beginWrite().forEachKey { entry ->
            consumer.accept(entry.key)
            true
        }
    }

    // Persistent cache impl
    override fun createNextVersion(): PersistentCache<K, V> {
        val nextVersion = versionTracker.nextVersion()
        // Copy key index for the next cache
        // It effectively prohibits new version from seeing new values cached for previous versions
        // as they might be stale, e.g. due to values already changed by another transaction
        val keyVersionIndexCopy = keyVersionIndex.clone
        val indexCopy = keyIndex?.clone()

        return CaffeinePersistentCache(
            cache, config,
            nextVersion,
            versionTracker,
            evictionSubject,
            keyVersionIndexCopy,
            indexCopy
        )
    }

    override fun registerClient(): PersistentCacheClient {
        val client = object : PersistentCacheClient {

            private var unregistered = false

            override fun unregister() {
                check(!unregistered) { "Client already unregistered" }
                versionTracker.decrementClients(version)
                unregistered = true
            }
        }
        versionTracker.incrementClients(version)
        return client
    }

    override fun release() {
        evictionSubject.removeListener(evictionListener)
    }

    // For tests
    fun localIndexSize(): Int {
        return keyVersionIndex.current.size()
    }

    // PersistentHashMap extensions
    private fun <K : Any, V> PersistentHashMap<K, V>.removeKey(key: K) {
        update { it.removeKey(key) }
    }

    private fun <K : Any, V : Any> PersistentHashMap<K, V>.put(key: K, value: V) {
        update { it.put(key, value) }
    }

    private fun <K, V> PersistentHashMap<K, V>.update(block: (PersistentHashMap<K, V>.MutablePersistentHashMap) -> Unit) {
        val mutableMap = beginWrite()
        block(mutableMap)
        endWrite(mutableMap)
    }

    // Cache extensions
    private fun Cache<K, WeightedValueMap<Version, V>>.getVersioned(key: K, version: Version): V? {
        return this.getIfPresent(key)?.get(version)
    }

    // WeightedValueMap extensions
    // Returns true if values were changed
    private fun WeightedValueMap<Version, V>.removeStaleVersions(currentVersion: Version): Boolean {
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

    private fun WeightedValueMap<Version, V>.removeStaleVersionsAndUpdateCache(key: K, currentVersion: Version) {
        if (removeStaleVersions(currentVersion)) {
            cache.put(key, this)
        }
    }
}