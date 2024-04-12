package jetbrains.exodus.entitystore.orientdb.iterate

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.query.OClassSelect
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OEntityOfTypeIterable(
    txn: StoreTransaction,
    private val entityType: String,
) : OQueryEntityIterableBase(txn) {

    override fun query(): OSelect {
        return OClassSelect(entityType)
    }
}
