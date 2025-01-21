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
package jetbrains.exodus.entitystore.youtrackdb.iterate.property

import jetbrains.exodus.entitystore.youtrackdb.YTDBStoreTransaction
import jetbrains.exodus.entitystore.youtrackdb.iterate.YTDBEntityIterableBase
import jetbrains.exodus.entitystore.youtrackdb.query.YTDBClassSelect
import jetbrains.exodus.entitystore.youtrackdb.query.YTDBFieldExistsCondition
import jetbrains.exodus.entitystore.youtrackdb.query.YTDBOrderByFields
import jetbrains.exodus.entitystore.youtrackdb.query.YTDBSelect

class YTDBPropertyExistsSortedIterable(
    txn: YTDBStoreTransaction,
    private val entityType: String,
    private val propertyName: String,
) : YTDBEntityIterableBase(txn) {

    override fun query(): YTDBSelect {
        val condition = YTDBFieldExistsCondition(propertyName)
        val order = YTDBOrderByFields(propertyName)
        return YTDBClassSelect(entityType, condition, order)
    }
}
