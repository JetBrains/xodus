package jetbrains.exodus.entitystore.orientdb.iterate.binop

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.orientdb.OEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OQueryFunctions
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OMinusEntityIterable(
    txn: StoreTransaction?,
    private val left: EntityIterableBase,
    private val right: EntityIterableBase
) : OEntityIterableBase(txn) {

    override fun query(): OSelect {
        if (left !is OEntityIterable || right !is OEntityIterable) {
            throw UnsupportedOperationException("UnionIterable is only supported for OEntityIterable")
        }
        return OQueryFunctions.difference(left.query(), right.query())
    }
}