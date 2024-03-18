package jetbrains.exodus.entitystore.iterate

import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.orientdb.OAllSelect
import jetbrains.exodus.entitystore.orientdb.OQuery

class OEntityOfTypeIterable(
    txn: PersistentStoreTransaction,
    private val entityType: String,
) : OEntityIterableBase(txn) {

    override fun query(): OQuery {
        return OAllSelect(entityType)
    }
}
