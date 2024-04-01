package jetbrains.exodus.entitystore.orientdb.iterate

import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.orientdb.query.OClassSelect
import jetbrains.exodus.entitystore.orientdb.query.ODistinctSelect
import jetbrains.exodus.entitystore.orientdb.query.OQuery

class ODistinctEntityIterable(
    txn: PersistentStoreTransaction,
    private val source: OEntityIterableBase,
) : OEntityIterableBase(txn) {

    override fun query(): OQuery {
        val sourceQuery = source.query()
        return ODistinctSelect(sourceQuery as OClassSelect)
    }
}