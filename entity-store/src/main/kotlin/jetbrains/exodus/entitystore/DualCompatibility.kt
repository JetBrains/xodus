package jetbrains.exodus.entitystore

import jetbrains.exodus.entitystore.orientdb.OEntityStore
import jetbrains.exodus.entitystore.orientdb.OQueryEntityIterable
import jetbrains.exodus.entitystore.orientdb.OStoreTransaction


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

fun StoreTransaction.asOStoreTransaction(): OStoreTransaction {
    return this as OStoreTransaction
}

fun EntityIterable.asOQueryIterable(): OQueryEntityIterable {
    require(this is OQueryEntityIterable) { "Only OEntityIterableBase is supported, but was ${this.javaClass.simpleName}" }
    return this
}

fun EntityStore.asOStore(): OEntityStore {
    require(this is OEntityStore) { "Only OEntityStore is supported, but was ${this.javaClass.simpleName}" }
    return this
}