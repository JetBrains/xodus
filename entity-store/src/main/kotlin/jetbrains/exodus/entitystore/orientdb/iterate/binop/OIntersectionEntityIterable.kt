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
package jetbrains.exodus.entitystore.orientdb.iterate.binop

import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.orientdb.OEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OQueries
import jetbrains.exodus.entitystore.orientdb.query.OQuery

class OIntersectionEntityIterable(
    txn: PersistentStoreTransaction?,
    private val left: EntityIterableBase,
    private val right: EntityIterableBase
) : OEntityIterableBase(txn) {

    override fun query(): OQuery {
        if (left !is OEntityIterable || right !is OEntityIterable) {
            throw UnsupportedOperationException("UnionIterable is only supported for OEntityIterable")
        }
        return OQueries.intersect(left.query(), right.query())
    }
}
