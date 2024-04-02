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
package jetbrains.exodus.entitystore.iterate.property

import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OClassSelect
import jetbrains.exodus.entitystore.orientdb.query.OOrderByField
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OPropertySortedIterable(
    txn: StoreTransaction,
    private val entityType: String,
    private val propertyName: String,
    private val ascending: Boolean,
    private val source: OEntityIterableBase? = null
) : OEntityIterableBase(txn) {

    override fun query(): OSelect {
        if (source != null) {
            return source.query().withOrder(propertyName, ascending)
        } else {
            val order = OOrderByField(propertyName, ascending)
            return OClassSelect(entityType, condition = null, order)
        }
    }
}
