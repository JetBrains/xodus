/*
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
package jetbrains.exodus.entitystore.util

import jetbrains.exodus.core.dataStructures.hash.LongSet
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.iterate.EntityIdSet


internal open class EmptyEntityIdSet : EntityIdSet {

    override fun add(id: EntityId?): EntityIdSet = SingleTypeEntityIdSet(id)

    override fun add(typeId: Int, localId: Long): EntityIdSet = SingleTypeEntityIdSet(typeId, localId)

    override fun contains(id: EntityId?) = false

    override fun contains(typeId: Int, localId: Long) = false

    override fun remove(id: EntityId?) = false

    override fun remove(typeId: Int, localId: Long) = false

    override fun count() = 0

    override fun getTypeSetSnapshot(typeId: Int) = LongSet.EMPTY

    override fun isEmpty() = true

    override fun iterator() = NOTHING.iterator()

    companion object {
        private val NOTHING = mutableSetOf<EntityId>()
    }
}