package jetbrains.exodus.entitystore.orientdb.iterate.binop

import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.orientdb.OEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OClassSelect
import jetbrains.exodus.entitystore.orientdb.query.ODifferenceSelect
import jetbrains.exodus.entitystore.orientdb.query.OQuery

class OMinusEntityIterable(
    txn: PersistentStoreTransaction?,
    private val left: EntityIterableBase,
    private val right: EntityIterableBase
) : OEntityIterableBase(txn) {

    override fun query(): OQuery {
        if (left !is OEntityIterable || right !is OEntityIterable) {
            throw UnsupportedOperationException("UnionIterable is only supported for OEntityIterable")
        }
        return ODifferenceSelect(left.query() as OClassSelect, right.query() as OClassSelect)
    }
}