package jetbrains.exodus.entitystore.orientdb.iterate.binop

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.OQueryEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OSelect
import jetbrains.exodus.entitystore.orientdb.query.OUnionSelect

class OConcatEntityIterable(
    txn: StoreTransaction?,
    private val iterable1: OQueryEntityIterable,
    private val iterable2: OQueryEntityIterable
) : OQueryEntityIterableBase(txn) {

    override fun query(): OSelect {
        return OUnionSelect(iterable1.query(), iterable2.query())
    }
}

