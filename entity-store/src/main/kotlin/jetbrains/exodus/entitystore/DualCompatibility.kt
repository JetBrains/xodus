package jetbrains.exodus.entitystore

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