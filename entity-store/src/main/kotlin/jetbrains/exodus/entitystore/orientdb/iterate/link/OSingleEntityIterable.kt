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
import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.OEntityId
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OClassSelect
import jetbrains.exodus.entitystore.orientdb.query.OSelect
import jetbrains.exodus.entitystore.orientdb.query.OSingleVertexCondition

open class OSingleEntityIterable(tx: StoreTransaction?, val entity: Entity) : OQueryEntityIterableBase(tx) {
    override fun query(): OSelect {
        return OClassSelect(
            className = entity.type,
            condition = OSingleVertexCondition((entity.id as OEntityId).asOId().toString())
        )
    }
}
