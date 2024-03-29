package jetbrains.exodus.entitystore.orientdb.iterate

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.query.OAllSelect
import jetbrains.exodus.entitystore.orientdb.query.OQuery

class OEntityOfTypeIterable(
    txn: StoreTransaction,
    private val entityType: String,
) : OEntityIterableBase(txn) {

    override fun query(): OQuery {
        return OAllSelect(entityType)
    }
}
