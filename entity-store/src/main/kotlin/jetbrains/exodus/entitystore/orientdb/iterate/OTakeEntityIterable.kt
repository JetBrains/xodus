package jetbrains.exodus.entitystore.orientdb.iterate

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.OQueryEntityIterable
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OTakeEntityIterable(
    txn: StoreTransaction?,
    private val source: OQueryEntityIterable,
    private val take: Int
) : OQueryEntityIterableBase(txn) {

    override fun query(): OSelect {
        return source.query().withLimit(take)
    }
}