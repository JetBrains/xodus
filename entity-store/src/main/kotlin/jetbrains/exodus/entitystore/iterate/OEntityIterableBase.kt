package jetbrains.exodus.entitystore.iterate

import jetbrains.exodus.entitystore.PersistentStoreTransaction

abstract class OEntityIterableBase(tx: PersistentStoreTransaction) : EntityIterableBase(tx) {

    override fun isSortedById() = false
    override fun canBeCached() = false
}