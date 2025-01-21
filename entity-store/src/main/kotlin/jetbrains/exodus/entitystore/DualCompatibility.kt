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
package jetbrains.exodus.entitystore

import jetbrains.exodus.entitystore.youtrackdb.YTDBEntityIterable
import jetbrains.exodus.entitystore.youtrackdb.YTDBEntityStore
import jetbrains.exodus.entitystore.youtrackdb.YTDBStoreTransaction


/**
 * This method is used where a [PersistentStoreTransaction] is expected but a [StoreTransaction] is provided.
 */
fun StoreTransaction.asPersistent(): PersistentStoreTransaction {
    return this as PersistentStoreTransaction
}

fun Entity.asPersistent(): PersistentEntity {
    return this as PersistentEntity
}

fun PersistentEntityStore.asPersistent(): PersistentEntityStoreImpl {
    return this as PersistentEntityStoreImpl
}

fun StoreTransaction.asOStoreTransaction(): YTDBStoreTransaction {
    return this as YTDBStoreTransaction
}

fun EntityIterable.asOQueryIterable(): YTDBEntityIterable {
    require(this is YTDBEntityIterable) { "Only OEntityIterableBase is supported, but was ${this.javaClass.simpleName}" }
    return this
}

fun EntityStore.asOStore(): YTDBEntityStore {
    require(this is YTDBEntityStore) { "Only OEntityStore is supported, but was ${this.javaClass.simpleName}" }
    return this
}