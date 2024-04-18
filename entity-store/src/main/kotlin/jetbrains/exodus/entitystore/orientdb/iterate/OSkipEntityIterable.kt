package jetbrains.exodus.entitystore.orientdb.iterate

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.OQueryEntityIterable
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OSkipEntityIterable(
    txn: StoreTransaction?,
    private val source: OQueryEntityIterable,
    private val skip: Int
) : OQueryEntityIterableBase(txn) {

    override fun query(): OSelect {
        return source.query().withSkip(skip)
    }
}