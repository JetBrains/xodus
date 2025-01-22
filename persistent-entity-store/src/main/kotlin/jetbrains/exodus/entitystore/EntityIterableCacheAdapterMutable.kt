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

internal class EntityIterableCacheAdapterMutable private constructor(
    config: PersistentEntityStoreConfig,
    cache: PersistentCache<EntityIterableHandle, CachedInstanceIterable>,
    stickyObjects: HashMap<EntityIterableHandle, Updatable>,
) : EntityIterableCacheAdapter(config, cache, stickyObjects) {

    companion object {

        fun cloneFrom(cacheAdapter: EntityIterableCacheAdapter): EntityIterableCacheAdapterMutable {
            val oldCache = cacheAdapter.cache
            val newCache = oldCache.createNextVersion()
            val stickyObjects = HashMap(cacheAdapter.stickyObjects)
            return EntityIterableCacheAdapterMutable(cacheAdapter.config, newCache, stickyObjects)
        }
    }

    fun endWrite(): EntityIterableCacheAdapter {
        return EntityIterableCacheAdapter(config, cache, stickyObjects)
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
        val index = cache.externalIndex as EntityIterableCacheReverseIndex
        when {
            checker.linkId >= 0 -> index.getLinkIdHandles(checker.linkId)?.forEach(action)
            checker.propertyId >= 0 -> index.getPropertyIdHandles(checker.propertyId)?.forEach(action)
            checker.typeIdAffectingCreation >= 0 -> index.getTypeIdAffectingCreationHandles(checker.typeIdAffectingCreation)?.forEach(action)
            checker.typeId >= 0 -> index.getEntityTypeIdHandles(checker.typeId)?.forEach(action)
            else -> cache.forEachKey(action)
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
}
