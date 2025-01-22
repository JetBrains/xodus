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
package jetbrains.exodus.entitystore.iterate

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.asPersistent
import jetbrains.exodus.env.Cursor
import jetbrains.exodus.kotlin.notNull

abstract class PropertyRangeOrValueIterableBase(
    txn: StoreTransaction,
    private val entityTypeId: Int,
    val propertyId: Int
) : EntityIterableBase(txn) {

    private var propertiesIterable: PropertiesIterable? = null
        get() {
            return field ?: PropertiesIterable(transaction, entityTypeId, propertyId).also { field = it }
        }

    override fun getEntityTypeId() = entityTypeId

    override fun canBeCached() = !propertiesIterable.notNull.isCached

    protected fun openCursor(txn: StoreTransaction): Cursor? {
        return storeImpl.getPropertyValuesIndexCursor(txn.asPersistent(), entityTypeId, propertyId)
    }

    protected val propertyValueIndex: EntityIterableBase get() = propertiesIterable.notNull.asProbablyCached()
}
