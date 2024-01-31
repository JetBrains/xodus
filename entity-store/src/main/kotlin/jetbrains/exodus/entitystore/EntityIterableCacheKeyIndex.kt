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

import jetbrains.exodus.core.cache.persistent.PersistentKeyIndex
import jetbrains.exodus.core.dataStructures.persistent.PersistentHashMap
import jetbrains.exodus.core.dataStructures.persistent.PersistentHashSet

typealias FieldIdGroupedHandleMap = PersistentHashMap<Int, PersistentHashSet<EntityIterableHandle>>

class EntityIterableCacheKeyIndex(
    val byLink: FieldIdGroupedHandleMap = FieldIdGroupedHandleMap(),
    val byProp: FieldIdGroupedHandleMap = FieldIdGroupedHandleMap(),
    val byTypeId: FieldIdGroupedHandleMap = FieldIdGroupedHandleMap(),
    val byTypeIdAffectingCreation: FieldIdGroupedHandleMap = FieldIdGroupedHandleMap()
) : PersistentKeyIndex<EntityIterableHandle> {

    override fun put(handle: EntityIterableHandle) {
        byLink.add(handle, handle.linkIds)
        byProp.add(handle, handle.propertyIds)
        byTypeId.add(handle, handle.entityTypeId)
        byTypeIdAffectingCreation.add(handle, handle.typeIdsAffectingCreation)
    }

    override fun remove(key: EntityIterableHandle) {
        byLink.remove(key, key.linkIds)
        byProp.remove(key, key.propertyIds)
        byTypeId.remove(key, key.entityTypeId)
        byTypeIdAffectingCreation.remove(key, key.typeIdsAffectingCreation)
    }

    override fun clone(): PersistentKeyIndex<EntityIterableHandle> {
        return EntityIterableCacheKeyIndex(
            byLink.cloneAll(),
            byProp.cloneAll(),
            byTypeId.cloneAll(),
            byTypeIdAffectingCreation.cloneAll(),
        )
    }

    private fun FieldIdGroupedHandleMap.cloneAll(): FieldIdGroupedHandleMap {
        val clone = this.clone
        val mutable = clone.beginWrite()
        mutable.forEachKey { entry ->
            val fieldId = entry.key
            val handles = entry.value
            mutable.put(fieldId, handles.clone)
            true
        }
        clone.endWrite(mutable)
        return clone
    }

    private fun FieldIdGroupedHandleMap.add(handle: EntityIterableHandle, fieldIds: IntArray) {
        for (fieldId in fieldIds) {
            if (fieldId >= 0) {
                add(handle, fieldId)
            }
        }
    }

    private fun FieldIdGroupedHandleMap.add(handle: EntityIterableHandle, fieldId: Int) {
        var handles = current.get(fieldId)
        if (handles == null) {
            handles = PersistentHashSet<EntityIterableHandle>()
            beginWrite().apply {
                put(fieldId, handles)
                endWrite(this)
            }
        }
        handles.beginWrite().apply {
            add(handle)
            handles.endWrite(this)
        }
    }

    private fun FieldIdGroupedHandleMap.remove(handle: EntityIterableHandle, fieldIds: IntArray) {
        for (fieldId in fieldIds) {
            if (fieldId >= 0) {
                remove(handle, fieldId)
            }
        }
    }

    private fun FieldIdGroupedHandleMap.remove(handle: EntityIterableHandle, fieldId: Int) {
        current.get(fieldId)?.let { handles ->
            handles.beginWrite().apply {
                remove(handle)
                handles.endWrite(this)
            }
            if (handles.isEmpty()) {
                beginWrite().apply {
                    removeKey(fieldId)
                    endWrite(this)
                }
            }
        }
    }
}

fun FieldIdGroupedHandleMap.forEachHandle(fieldId: Int, action: (EntityIterableHandle) -> Unit) {
    current.get(fieldId)?.let { handles ->
        for (handle in handles) {
            action(handle)
        }
    }
}