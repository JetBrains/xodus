/**
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore

import jetbrains.exodus.core.dataStructures.hash.HashMap
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure
import jetbrains.exodus.core.dataStructures.persistent.EvictListener
import jetbrains.exodus.core.dataStructures.persistent.PersistentObjectCache
import jetbrains.exodus.core.execution.SharedTimer.ExpirablePeriodicTask
import jetbrains.exodus.entitystore.iterate.CachedInstanceIterable
import java.lang.ref.SoftReference

internal open class EntityIterableCacheAdapter
@JvmOverloads constructor(internal val config: PersistentEntityStoreConfig,
                          internal val cache: NonAdjustablePersistentObjectCache<EntityIterableHandle, CacheItem> = NonAdjustablePersistentObjectCache(config.entityIterableCacheSize),
                          internal val stickyObjects: HashMap<EntityIterableHandle, Updatable> = HashMap()) {

    fun tryKey(key: EntityIterableHandle): CachedInstanceIterable? {
        if (key.isSticky) {
            return getStickyObject(key) as CachedInstanceIterable
        }
        return getStickyObjectUnsafe(key) as CachedInstanceIterable? ?: parseCachedObject(key, cache.tryKey(key))
    }

    fun getObject(key: EntityIterableHandle): CachedInstanceIterable? {
        if (key.isSticky) {
            return getStickyObject(key) as CachedInstanceIterable
        }
        return getStickyObjectUnsafe(key) as CachedInstanceIterable? ?: parseCachedObject(key, cache.getObject(key))
    }

    fun getUpdatable(key: EntityIterableHandle): Updatable? {
        if (key.isSticky) {
            return getStickyObject(key)
        }
        return getStickyObjectUnsafe(key) ?: parseCachedObject(key, cache.getObject(key)) as Updatable?
    }

    open fun cacheObject(key: EntityIterableHandle, it: CachedInstanceIterable) {
        // if it is Updatable then it could be mutated in a txn being stored as sticky object
        if (it is Updatable && stickyObjects.containsKey(key)) {
            stickyObjects[key] = it
        } else {
            cache.cacheObject(key, CacheItem(it, config.entityIterableCacheMaxSizeOfDirectValue))
        }
    }

    fun forEachKey(procedure: ObjectProcedure<EntityIterableHandle>) = cache.forEachKey(procedure)

    open fun remove(key: EntityIterableHandle) {
        cache.remove(key)
    }

    fun hitRate() = cache.hitRate()

    fun count() = cache.count()

    fun size() = cache.size()

    open fun clear() = cache.clear()

    val isSparse: Boolean get() = cache.count() < cache.size() / 2

    val clone: EntityIterableCacheAdapterMutable get() = EntityIterableCacheAdapterMutable.create(this)

    fun adjustHitRate() = cache.adjustHitRate()

    fun getStickyObjectUnsafe(handle: EntityIterableHandle): Updatable? = stickyObjects[handle]

    fun getStickyObject(handle: EntityIterableHandle): Updatable {
        return getStickyObjectUnsafe(handle) ?: throw IllegalStateException(
            "Sticky object not found, handle=${EntityIterableCache.toString(config, handle)}"
        )
    }

    private fun parseCachedObject(key: EntityIterableHandle, item: CacheItem?): CachedInstanceIterable? {
        return item?.let {
            var cached = it.cached
            if (cached == null) {
                cached = it.ref?.get()
                if (cached == null) {
                    cache.remove(key)
                }
            }
            return cached
        }
    }

    internal class CacheItem(it: CachedInstanceIterable, maxSizeOfDirectValue: Int) {

        var cached: CachedInstanceIterable? = null
        var ref: SoftReference<CachedInstanceIterable>? = null

        init {
            if (it.isUpdatable || it.size() <= maxSizeOfDirectValue) {
                cached = it
                ref = null
            } else {
                cached = null
                ref = SoftReference(it)
            }
        }
    }

    /*
    NonAdjustablePersistentObjectCache doesn't adjust itself in order to avoid as many cache adjusters
    as many versions of the cache (as a persistent data structure) can be.
     */
    internal class NonAdjustablePersistentObjectCache<K, V> : PersistentObjectCache<K, V> {

        constructor(size: Int) : super(size)
        constructor(source: NonAdjustablePersistentObjectCache<K, V>, listener: EvictListener<K, V>?) : super(
            source,
            listener
        )

        override fun getClone(listener: EvictListener<K, V>?): NonAdjustablePersistentObjectCache<K, V> {
            return NonAdjustablePersistentObjectCache(this, listener)
        }

        fun endWrite(): NonAdjustablePersistentObjectCache<K, V> {
            return NonAdjustablePersistentObjectCache(this, null)
        }

        override fun getCacheAdjuster(): ExpirablePeriodicTask? = null
    }

    companion object {

        fun getCachedValue(item: CacheItem): CachedInstanceIterable? {
            var cached = item.cached
            if (cached == null) {
                cached = item.ref?.get()
            }
            return cached
        }
    }

}

