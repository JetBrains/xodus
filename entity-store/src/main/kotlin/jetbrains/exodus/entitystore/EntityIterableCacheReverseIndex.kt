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

import jetbrains.exodus.core.cache.persistent.PersistentIndex
import jetbrains.exodus.core.dataStructures.persistent.PersistentHashMap
import jetbrains.exodus.core.dataStructures.persistent.PersistentHashSet

typealias HandleIndex = PersistentHashMap<Int, PersistentHashSet<EntityIterableHandle>>
typealias MutableHandleIndex = PersistentHashMap<Int, PersistentHashSet<EntityIterableHandle>>.MutablePersistentHashMap

class EntityIterableCacheReverseIndex(
    // Maps representing the reversed index distribution of the entity iterable handles
    private val linkIds: HandleIndex = HandleIndex(),
    private val propertyIds: HandleIndex = HandleIndex(),
    private val typeIdsAffectingCreation: HandleIndex = HandleIndex(),
    private val entityTypeId: HandleIndex = HandleIndex()
) : PersistentIndex<EntityIterableHandle> {

    override fun add(handle: EntityIterableHandle) {
        addToMap(handle, linkIds, handle.linkIds)
        addToMap(handle, propertyIds, handle.propertyIds)
        addToMap(handle, typeIdsAffectingCreation, handle.typeIdsAffectingCreation)
        addToMap(handle, entityTypeId, intArrayOf(handle.entityTypeId))
    }

    private fun addToMap(handle: EntityIterableHandle, map: HandleIndex, ids: IntArray) {
        if (ids.isEmpty()) {
            return
        }
        val mutable = map.beginWrite()
        ids.forEach { id -> addHandle(id, handle, mutable) }
        mutable.endWrite()
    }

    private fun addHandle(id: Int, handle: EntityIterableHandle, mutable: MutableHandleIndex) {
        var set = mutable.get(id)?.clone ?: PersistentHashSet<EntityIterableHandle>()
        set.beginWrite().apply { add(handle); endWrite() }
        mutable.put(id, set)
    }

    override fun remove(handle: EntityIterableHandle) {
        removeFromMap(handle, linkIds, handle.linkIds)
        removeFromMap(handle, propertyIds, handle.propertyIds)
        removeFromMap(handle, typeIdsAffectingCreation, handle.typeIdsAffectingCreation)
        removeFromMap(handle, entityTypeId, intArrayOf(handle.entityTypeId))
    }

    private fun removeFromMap(handle: EntityIterableHandle, map: HandleIndex, ids: IntArray) {
        if (ids.isEmpty()) {
            return
        }
        val mutable = map.beginWrite()
        ids.forEach { id -> removeHandle(id, handle, mutable) }
        mutable.endWrite()
    }

    private fun removeHandle(id: Int, handle: EntityIterableHandle, mutable: MutableHandleIndex) {
        var set = mutable.get(id)?.clone ?: return
        set.beginWrite().apply { remove(handle); endWrite() }
        mutable.put(id, set)
    }

    fun getLinkIdHandles(linkId: Int): Iterable<EntityIterableHandle>? {
        return linkIds.current.get(linkId)
    }

    fun getPropertyIdHandles(propertyId: Int): Iterable<EntityIterableHandle>? {
        return propertyIds.current.get(propertyId)
    }

    fun getTypeIdAffectingCreationHandles(typeId: Int): Iterable<EntityIterableHandle>? {
        return typeIdsAffectingCreation.current.get(typeId)
    }

    fun getEntityTypeIdHandles(entityTypeId: Int): Iterable<EntityIterableHandle>? {
        return this.entityTypeId.current.get(entityTypeId)
    }

    override fun clone(): EntityIterableCacheReverseIndex {
        return EntityIterableCacheReverseIndex(
            linkIds.clone,
            propertyIds.clone,
            typeIdsAffectingCreation.clone,
            entityTypeId.clone
        )
    }
}