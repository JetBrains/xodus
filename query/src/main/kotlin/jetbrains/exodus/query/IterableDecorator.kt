/**
 * Copyright 2010 - 2019 JetBrains s.r.o.
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
package jetbrains.exodus.query

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.query.metadata.ModelMetaData

class IterableDecorator(iterable: Iterable<Entity>) : NodeBase() {

    private val it = StaticTypedEntityIterable.instantiate(iterable)

    override fun instantiate(entityType: String, queryEngine: QueryEngine, metaData: ModelMetaData): Iterable<Entity> {
        metaData.getEntityMetaData(entityType)?.let { emd ->
            if (!emd.hasSubTypes() && emd.superType == null) {
                return it
            }
        }
        val entityStore = queryEngine.persistentStore
        val txn = entityStore.andCheckCurrentTransaction
        if (it is EntityIterableBase) {
            return queryEngine.wrap(it.source.intersect(queryEngine.instantiateGetAll(txn, entityType)))
        }
        val typeId = entityStore.getEntityTypeId(txn, entityType, false)
        if (it is List<*>) {
            return it.filter { entity -> entity.id.typeId == typeId }
        }
        return it.asSequence().filter { entity -> entity.id.typeId == typeId }.asIterable()
    }

    override fun getClone() = IterableDecorator(it)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        other?.let { r ->
            return r is IterableDecorator &&
                    (it === r.it || (it is EntityIterableBase && r.it is EntityIterableBase && it.source.handle == r.it.source.handle))
        }
        return false
    }

    override fun getHandle(sb: StringBuilder): StringBuilder {
        super.getHandle(sb).append('(')
        if (it is EntityIterableBase) {
            sb.append(it.source.handle.toString())
        } else {
            sb.append(it.hashCode() and 0x7fffffff)
        }
        return sb.append(')')
    }

    override fun getSimpleName() = "id"

    override fun canBeCached() = false
}