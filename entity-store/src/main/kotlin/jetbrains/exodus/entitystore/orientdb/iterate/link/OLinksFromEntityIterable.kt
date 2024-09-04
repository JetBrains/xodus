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

import jetbrains.exodus.entitystore.orientdb.OEntityId
import jetbrains.exodus.entitystore.orientdb.OStoreTransaction
import jetbrains.exodus.entitystore.orientdb.asEdgeClass
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OLinkOutFromIdSelect
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OLinksFromEntityIterable(
    txn: OStoreTransaction,
    private val linkName: String,
    private val fromEntityId: OEntityId,
) : OEntityIterableBase(txn) {

    override fun query(): OSelect {
        return OLinkOutFromIdSelect(linkName.asEdgeClass, listOf(fromEntityId.asOId()))
    }
}
