package jetbrains.exodus.entitystore.orientdb.iterate

import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.orientdb.query.OClassSelect
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OEntityOfTypeIterable(
    txn: PersistentStoreTransaction,
    private val entityType: String,
) : OEntityIterableBase(txn) {

    override fun query(): OSelect {
        return OClassSelect(entityType)
    }
}
