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
package jetbrains.exodus.entitystore.iterate.binop

import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.OEntityIterable
import jetbrains.exodus.entitystore.orientdb.OQuery

class OIntersectionIterable(
    txn: PersistentStoreTransaction?,
    private val iterable1: EntityIterableBase,
    private val iterable2: EntityIterableBase
) : OEntityIterableBase(txn) {

    override fun query(): OQuery {
        if (iterable1 !is OEntityIterable || iterable2 !is OEntityIterable) {
            throw UnsupportedOperationException("UnionIterable is only supported for OEntityIterable")
        }
        return iterable1.query().intersect(iterable2.query())
    }
}