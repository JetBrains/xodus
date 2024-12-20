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
package jetbrains.exodus.entitystore.orientdb.iterate.link

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.orientdb.OEntityId
import jetbrains.exodus.entitystore.orientdb.OStoreTransaction
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.ORecordIdSelect
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OMultipleEntitiesIterable(tx: OStoreTransaction, val entities: List<Entity>) : OEntityIterableBase(tx) {
    override fun query(): OSelect {
        return ORecordIdSelect(entities.map { (it.id as OEntityId).asOId() })
    }

    override fun contains(entity: Entity): Boolean {
        return entities.contains(entity)
    }

    override fun count(): Long {
        return entities.size.toLong()
    }

    override fun getRoughCount(): Long {
        return entities.size.toLong()
    }

    override fun getRoughSize(): Long {
        return entities.size.toLong()
    }

    override fun size(): Long {
        return entities.size.toLong()
    }

    override fun skip(number: Int): EntityIterable {
        return OMultipleEntitiesIterable(transaction as OStoreTransaction, entities.drop(number) )
    }

    override fun take(number: Int): EntityIterable {
        return OMultipleEntitiesIterable(transaction as OStoreTransaction, entities.take(number) )
    }
}
