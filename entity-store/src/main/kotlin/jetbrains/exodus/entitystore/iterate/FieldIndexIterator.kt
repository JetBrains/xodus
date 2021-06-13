/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore.iterate

import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.PersistentEntityId

class FieldIndexIterator(
    it: EntityIterableBase,
    private val entityTypeId: Int,
    private val fieldId: Int,
    iterable: Iterable<Pair<Int, Long>>
) : EntityIteratorBase(it) {

    private val iterator = iterable.iterator()
    private var entityId: EntityId? = null

    init {
        advance()
    }

    override fun hasNextImpl() = entityId != null

    override fun nextIdImpl(): EntityId? {
        if (hasNextImpl()) {
            val entityId = entityId
            advance()
            return entityId
        }
        return null
    }

    private fun advance() {
        entityId = null
        if (iterator.hasNext()) {
            val next = iterator.next()
            if (next.first == fieldId) {
                entityId = PersistentEntityId(entityTypeId, next.second)
            }
        }
    }
}