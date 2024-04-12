package jetbrains.exodus.entitystore.orientdb.iterate.binop

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.OQueryEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OQueryFunctions
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OMinusEntityIterable(
    txn: StoreTransaction?,
    private val left: OQueryEntityIterable,
    private val right: OQueryEntityIterable
) : OQueryEntityIterableBase(txn) {

    override fun query(): OSelect {
        return OQueryFunctions.difference(left.query(), right.query())
    }
}