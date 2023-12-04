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

import jetbrains.exodus.core.dataStructures.hash.HashMap
import jetbrains.exodus.core.dataStructures.hash.HashSet
import jetbrains.exodus.core.dataStructures.hash.IntHashMap
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure
import jetbrains.exodus.core.dataStructures.persistent.EvictListener
import jetbrains.exodus.entitystore.PersistentStoreTransaction.HandleCheckerAdapter
import jetbrains.exodus.entitystore.iterate.CachedInstanceIterable
import jetbrains.exodus.entitystore.iterate.EntityIterableBase

internal class EntityIterableCacheAdapterMutable private constructor(config: PersistentEntityStoreConfig,
                                                                     private val handlesDistribution: HandlesDistribution,
                                                                     stickyObjects: HashMap<EntityIterableHandle, Updatable>) : EntityIterableCacheAdapter(config, handlesDistribution.cache, stickyObjects) {
    fun endWrite(): EntityIterableCacheAdapter {
        return EntityIterableCacheAdapter(config, cache.endWrite(), stickyObjects)
    }

    fun update(checker: HandleCheckerAdapter) {
        val procedure: ObjectProcedure<EntityIterableHandle> = ObjectProcedure {
            if (checker.checkHandle(it)) {
                remove(it)
            }
            true
        }
        when {
            checker.linkId >= 0 -> {
                handlesDistribution.byLink.forEachHandle(checker.linkId, procedure)
            }
            checker.propertyId >= 0 -> {
                handlesDistribution.byProp.forEachHandle(checker.propertyId, procedure)
            }
            checker.typeIdAffectingCreation >= 0 -> {
                handlesDistribution.byTypeIdAffectingCreation.forEachHandle(checker.typeIdAffectingCreation, procedure)
            }
            checker.typeId >= 0 -> {
                handlesDistribution.byTypeId.forEachHandle(checker.typeId, procedure)
                handlesDistribution.byTypeId.forEachHandle(EntityIterableBase.NULL_TYPE_ID, procedure)
            }
            else -> {
                forEachKey(procedure)
            }
        }
        for (handle in stickyObjects.keys) {
            checker.checkHandle(handle)
        }
    }

    override fun cacheObject(key: EntityIterableHandle, it: CachedInstanceIterable) {
        super.cacheObject(key, it)
        handlesDistribution.addHandle(key)
    }

    fun registerStickyObject(handle: EntityIterableHandle, updatable: Updatable) {
        stickyObjects[handle] = updatable
    }

    override fun remove(key: EntityIterableHandle) {
        check(!key.isSticky) { "Cannot remove sticky object" }
        super.remove(key)
        handlesDistribution.removeHandle(key)
    }

    override fun clear() {
        super.clear()
        handlesDistribution.clear()
    }

    fun cacheObjectNotAffectingHandleDistribution(handle: EntityIterableHandle, it: CachedInstanceIterable) {
        super.cacheObject(handle, it)
    }

    private class HandlesDistribution(cache: NonAdjustablePersistentObjectCache<EntityIterableHandle, CacheItem>) : EvictListener<EntityIterableHandle, CacheItem> {

        val cache: NonAdjustablePersistentObjectCache<EntityIterableHandle, CacheItem>
        val removed: MutableSet<EntityIterableHandle>
        val byLink: FieldIdGroupedHandles
        val byProp: FieldIdGroupedHandles
        val byTypeId: FieldIdGroupedHandles
        val byTypeIdAffectingCreation: FieldIdGroupedHandles

        init {
            this.cache = cache.getClone(this)
            val count = cache.count()
            // this set is intentionally created quite disperse in order to reduce number of calls
            // to EntityIterableHandle.equals() during iteration of handle clusters
            removed = HashSet(10, .33f)
            byLink = FieldIdGroupedHandles(count / 16, removed)
            byProp = FieldIdGroupedHandles(count / 16, removed)
            byTypeId = FieldIdGroupedHandles(count / 16, removed)
            byTypeIdAffectingCreation = FieldIdGroupedHandles(count / 16, removed)
            cache.forEachEntry { handle, value ->
                val iterable = getCachedValue(value)
                if (iterable != null) {
                    addHandle(handle)
                }
                true
            }
        }

        override fun onEvict(key: EntityIterableHandle, value: CacheItem) {
            removeHandle(key)
        }

        fun removeHandle(handle: EntityIterableHandle) = removed.add(handle)

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

    private class FieldIdGroupedHandles(capacity: Int,
                                        private val removed: Set<EntityIterableHandle>) : IntHashMap<MutableList<EntityIterableHandle>>(capacity) {

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

        fun forEachHandle(fieldId: Int, procedure: ObjectProcedure<EntityIterableHandle>) {
            get(fieldId)?.let { handles ->
                if (removed.isEmpty()) {
                    for (handle in handles) {
                        procedure.execute(handle)
                    }
                } else {
                    for (handle in handles) {
                        if (!removed.contains(handle)) {
                            procedure.execute(handle)
                        }
                    }
                }
            }
        }
    }

    companion object {

        fun create(source: EntityIterableCacheAdapter): EntityIterableCacheAdapterMutable {
            val handlesDistribution = HandlesDistribution(source.cache)
            return EntityIterableCacheAdapterMutable(source.config, handlesDistribution, HashMap(source.stickyObjects))
        }
    }

}
