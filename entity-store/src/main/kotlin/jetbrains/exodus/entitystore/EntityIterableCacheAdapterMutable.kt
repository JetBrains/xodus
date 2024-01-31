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

import jetbrains.exodus.core.cache.persistent.PersistentCache
import jetbrains.exodus.entitystore.PersistentStoreTransaction.HandleCheckerAdapter
import jetbrains.exodus.entitystore.iterate.CachedInstanceIterable
import jetbrains.exodus.entitystore.iterate.EntityIterableBase

internal class EntityIterableCacheAdapterMutable private constructor(
    config: PersistentEntityStoreConfig,
    cache: PersistentCache<EntityIterableHandle, CachedInstanceIterable>,
    stickyObjects: HashMap<EntityIterableHandle, Updatable>,
    cacheKeyIndex: EntityIterableCacheKeyIndex,
) : EntityIterableCacheAdapter(config, cache, stickyObjects, cacheKeyIndex) {


    companion object {

        fun cloneFrom(cacheAdapter: EntityIterableCacheAdapter): EntityIterableCacheAdapterMutable {
            val oldCache = cacheAdapter.cache
            val newCache = oldCache.createNextVersion()
            val stickyObjects = HashMap(cacheAdapter.stickyObjects)
            val keyIndex = newCache.keyIndex as EntityIterableCacheKeyIndex
            return EntityIterableCacheAdapterMutable(cacheAdapter.config, newCache, stickyObjects, keyIndex)
        }
    }

    override fun cacheObject(key: EntityIterableHandle, it: CachedInstanceIterable) {
        super.cacheObject(key, it)
    }

    fun cacheObjectNotAffectingHandleDistribution(handle: EntityIterableHandle, it: CachedInstanceIterable) {
        super.cacheObject(handle, it)
    }

    fun endWrite(): EntityIterableCacheAdapter {
        return EntityIterableCacheAdapter(config, cache, stickyObjects, cacheKeyIndex)
    }

    fun update(checker: HandleCheckerAdapter) {
        updateCacheWithChecker(checker)
        updateStickyObjectsWithChecker(checker)
    }

    private fun updateCacheWithChecker(checker: HandleCheckerAdapter) {
        val action: (EntityIterableHandle) -> Unit = {
            if (checker.checkHandle(it)) {
                remove(it)
            }
        }
        when {
            checker.linkId >= 0 -> {
                cacheKeyIndex.byLink.forEachHandle(checker.linkId, action)
            }

            checker.propertyId >= 0 -> {
                cacheKeyIndex.byProp.forEachHandle(checker.propertyId, action)
            }

            checker.typeIdAffectingCreation >= 0 -> {
                cacheKeyIndex.byTypeIdAffectingCreation.forEachHandle(checker.typeIdAffectingCreation, action)
            }

            checker.typeId >= 0 -> {
                cacheKeyIndex.byTypeId.forEachHandle(checker.typeId, action)
                cacheKeyIndex.byTypeId.forEachHandle(EntityIterableBase.NULL_TYPE_ID, action)
            }

            else -> {
                cache.forEachKey(action)
            }
        }
    }

    private fun updateStickyObjectsWithChecker(checker: HandleCheckerAdapter) {
        for (handle in stickyObjects.keys) {
            checker.checkHandle(handle)
        }
    }

    fun registerStickyObject(handle: EntityIterableHandle, updatable: Updatable) {
        stickyObjects[handle] = updatable
    }

    override fun remove(key: EntityIterableHandle) {
        check(!key.isSticky) { "Cannot remove sticky object" }
        super.remove(key)
    }

    override fun clear() {
        super.clear()
    }
}
