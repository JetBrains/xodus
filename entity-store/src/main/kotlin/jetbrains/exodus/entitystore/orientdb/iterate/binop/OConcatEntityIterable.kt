package jetbrains.exodus.entitystore.orientdb.iterate.binop

import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.OEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OSelect
import jetbrains.exodus.entitystore.orientdb.query.OUnionSelect

class OConcatEntityIterable(
    txn: StoreTransaction?,
    private val iterable1: EntityIterable,
    private val iterable2: EntityIterable
) : OEntityIterableBase(txn) {

    override fun query(): OSelect {
        if (iterable1 !is OEntityIterable || iterable2 !is OEntityIterable) {
            throw UnsupportedOperationException("ConcatIterable is only supported for OEntityIterable")
        }
        return OUnionSelect(iterable1.query(), iterable2.query())
    }
}

