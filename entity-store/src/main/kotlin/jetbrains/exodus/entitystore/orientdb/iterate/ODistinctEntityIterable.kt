package jetbrains.exodus.entitystore.orientdb.iterate

import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.orientdb.query.OQueryFunctions
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class ODistinctEntityIterable(
    txn: PersistentStoreTransaction,
    private val source: OEntityIterableBase,
) : OEntityIterableBase(txn) {

    override fun query(): OSelect {
        return OQueryFunctions.distinct(source.query())
    }
}