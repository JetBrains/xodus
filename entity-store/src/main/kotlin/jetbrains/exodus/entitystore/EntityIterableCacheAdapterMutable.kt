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
    private val handleDistribution: HandleDistribution
) : EntityIterableCacheAdapter(config, cache, stickyObjects) {

    companion object {

        fun cloneFrom(cacheAdapter: EntityIterableCacheAdapter): EntityIterableCacheAdapterMutable {
            val oldCache = cacheAdapter.cache
            val handleDistribution = HandleDistribution(oldCache.count().toInt())
            val newCache = oldCache.createNextVersion()
            newCache.forEachKey(handleDistribution::addHandle)
            val stickyObjects = HashMap(cacheAdapter.stickyObjects)
            return EntityIterableCacheAdapterMutable(cacheAdapter.config, newCache, stickyObjects, handleDistribution)
        }
    }

    fun endWrite(): EntityIterableCacheAdapter {
        return EntityIterableCacheAdapter(config, cache, stickyObjects)
    }

    override fun cacheObject(key: EntityIterableHandle, it: CachedInstanceIterable) {
        super.cacheObject(key, it)
        handleDistribution.addHandle(key)
    }

    fun cacheObjectNotAffectingHandleDistribution(handle: EntityIterableHandle, it: CachedInstanceIterable) {
        super.cacheObject(handle, it)
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
                handleDistribution.byLink.forEachHandle(checker.linkId, action)
            }

            checker.propertyId >= 0 -> {
                handleDistribution.byProp.forEachHandle(checker.propertyId, action)
            }

            checker.typeIdAffectingCreation >= 0 -> {
                handleDistribution.byTypeIdAffectingCreation.forEachHandle(checker.typeIdAffectingCreation, action)
            }

            checker.typeId >= 0 -> {
                handleDistribution.byTypeId.forEachHandle(checker.typeId, action)
                handleDistribution.byTypeId.forEachHandle(EntityIterableBase.NULL_TYPE_ID, action)
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
        handleDistribution.removeHandle(key)
    }

    override fun clear() {
        super.clear()
        handleDistribution.clear()
    }

    private class HandleDistribution(cacheCount: Int) {

        val removed: MutableSet<EntityIterableHandle>

        val byLink: FieldIdGroupedHandleMap
        val byProp: FieldIdGroupedHandleMap
        val byTypeId: FieldIdGroupedHandleMap
        val byTypeIdAffectingCreation: FieldIdGroupedHandleMap

        init {
            // this set is intentionally created quite disperse in order to reduce number of calls
            // to EntityIterableHandle.equals() during iteration of handle clusters
            removed = HashSet(10, .33f)
            byLink = FieldIdGroupedHandleMap(cacheCount / 16, removed)
            byProp = FieldIdGroupedHandleMap(cacheCount / 16, removed)
            byTypeId = FieldIdGroupedHandleMap(cacheCount / 16, removed)
            byTypeIdAffectingCreation = FieldIdGroupedHandleMap(cacheCount / 16, removed)
        }

        fun removeHandle(handle: EntityIterableHandle) {
            removed.add(handle)
        }

        fun addHandle(handle: EntityIterableHandle) {
            if (removed.isNotEmpty()) {
                removed.remove(handle)
            }
            byLink.add(handle, handle.linkIds)
            byProp.add(handle, handle.propertyIds)
            byTypeId.add(handle, handle.entityTypeId)
            byTypeIdAffectingCreation.add(handle, handle.typeIdsAffectingCreation)
        }

        fun clear() {
            removed.clear()
            byLink.clear()
            byProp.clear()
            byTypeId.clear()
            byTypeIdAffectingCreation.clear()
        }
    }

    private class FieldIdGroupedHandleMap(
        capacity: Int,
        private val removed: Set<EntityIterableHandle>
    ) : HashMap<Int, MutableList<EntityIterableHandle>>(capacity) {

        // it is allowed to add EntityIterableBase.NULL_TYPE_ID
        fun add(handle: EntityIterableHandle, fieldId: Int) {
            val handles = get(fieldId) ?: ArrayList<EntityIterableHandle>(4).also { put(fieldId, it) }
            handles.add(handle)
        }

        fun add(handle: EntityIterableHandle, fieldIds: IntArray) {
            for (fieldId in fieldIds) {
                if (fieldId >= 0) {
                    add(handle, fieldId)
                }
            }
        }

        fun forEachHandle(fieldId: Int, action: (EntityIterableHandle) -> Unit) {
            get(fieldId)?.let { handles ->
                if (removed.isEmpty()) {
                    for (handle in handles) {
                        action(handle)
                    }
                } else {
                    for (handle in handles) {
                        if (!removed.contains(handle)) {
                            action(handle)
                        }
                    }
                }
            }
        }
    }
}
