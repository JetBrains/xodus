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
package jetbrains.exodus.entitystore.youtrackdb.iterate.link

import jetbrains.exodus.entitystore.youtrackdb.YTDBEntityId
import jetbrains.exodus.entitystore.youtrackdb.YTDBStoreTransaction
import jetbrains.exodus.entitystore.youtrackdb.asEdgeClass
import jetbrains.exodus.entitystore.youtrackdb.iterate.YTDBEntityIterableBase
import jetbrains.exodus.entitystore.youtrackdb.query.YTDBLinkOfTypeInFromIdsSelect
import jetbrains.exodus.entitystore.youtrackdb.query.YTDBSelect

class YTDBLinkOfTypeToEntityIterable(
    txn: YTDBStoreTransaction,
    private val linkName: String,
    private val linkEntityId: YTDBEntityId,
    private val targetEntityType: String,
) : YTDBEntityIterableBase(txn) {

    override fun query(): YTDBSelect {
        return YTDBLinkOfTypeInFromIdsSelect(linkName.asEdgeClass, listOf(linkEntityId.asOId()), targetEntityType)
    }
}
