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
package jetbrains.exodus.entitystore

import jetbrains.exodus.core.cache.CaffeineCacheConfig
import jetbrains.exodus.core.cache.CaffeinePersistentCache
import jetbrains.exodus.core.cache.PersistentCache
import jetbrains.exodus.entitystore.iterate.CachedInstanceIterable
import java.time.Duration

internal open class EntityIterableCacheAdapter(
    val config: PersistentEntityStoreConfig,
    val cache: PersistentCache<EntityIterableHandle, CachedInstanceIterable>,
    val stickyObjects: HashMap<EntityIterableHandle, Updatable>
) {

    companion object {

        fun create(config: PersistentEntityStoreConfig): EntityIterableCacheAdapter {
            val cacheConfig = CaffeineCacheConfig<CachedInstanceIterable>(
                maxWeight = config.entityIterableCacheWeight,
                expireAfterAccess = Duration.ofSeconds(config.entityIterableCacheExpireAfterAccess.toLong()),
                useSoftValues = config.entityIterableCacheSoftValues,
                weigher = { it.roughSize.toInt() }
            )
            val cache = CaffeinePersistentCache.create<EntityIterableHandle, CachedInstanceIterable>(cacheConfig)

            return EntityIterableCacheAdapter(config, cache, HashMap())
        }
    }

    open fun getObject(key: EntityIterableHandle): CachedInstanceIterable? {
        if (key.isSticky) {
            return getStickyObject(key) as CachedInstanceIterable
        }
        return getStickyObjectUnsafe(key) as CachedInstanceIterable? ?: cache.get(key)
    }

    open fun getUpdatable(key: EntityIterableHandle): Updatable? {
        if (key.isSticky) {
            return getStickyObject(key)
        }
        return getStickyObjectUnsafe(key) ?: cache.get(key) as Updatable?
    }

    open fun cacheObject(key: EntityIterableHandle, it: CachedInstanceIterable) {
        // if it is Updatable then it could be mutated in a txn being stored as sticky object
        if (it is Updatable && stickyObjects.containsKey(key)) {
            stickyObjects[key] = it
        } else {
            cache.put(key, it)
        }
    }

    open fun remove(key: EntityIterableHandle) {
        cache.remove(key)
    }

    fun count() = cache.count()

    fun size() = cache.size()

    open fun clear() = cache.clear()

    val halfFull: Boolean get() = cache.count() > cache.size() / 2

    fun cloneToMutable(): EntityIterableCacheAdapterMutable {
        return EntityIterableCacheAdapterMutable.cloneFrom(this)
    }

    fun getStickyObjectUnsafe(handle: EntityIterableHandle): Updatable? {
        return stickyObjects[handle]
    }

    fun getStickyObject(handle: EntityIterableHandle): Updatable {
        return getStickyObjectUnsafe(handle) ?: throw IllegalStateException(
            "Sticky object not found, handle=${EntityIterableCache.toString(config, handle)}"
        )
    }
}