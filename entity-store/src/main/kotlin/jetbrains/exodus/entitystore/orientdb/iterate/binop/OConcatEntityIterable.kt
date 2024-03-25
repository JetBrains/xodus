package jetbrains.exodus.entitystore.orientdb.iterate.binop

import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.iterate.EntityIteratorBase
import jetbrains.exodus.entitystore.iterate.NonDisposableEntityIterator
import jetbrains.exodus.entitystore.orientdb.OEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OQueries
import jetbrains.exodus.entitystore.orientdb.query.OQuery

class OConcatEntityIterable(
    txn: PersistentStoreTransaction?,
    private val iterable1: EntityIterableBase,
    private val iterable2: EntityIterableBase
) : OEntityIterableBase(txn) {

    override fun query(): OQuery {
        if (iterable1 !is OEntityIterable || iterable2 !is OEntityIterable) {
            throw UnsupportedOperationException("ConcatIterable is only supported for OEntityIterable")
        }
        return OQueries.union(iterable1.query(), iterable2.query())
    }

    override fun size(): Long {
        return iterable1.size() + iterable2.size()
    }

    override fun isSortedById(): Boolean {
        return false
    }

    override fun countImpl(txn: PersistentStoreTransaction): Long {
        return iterable1.size() + iterable2.size()
    }

    override fun getIteratorImpl(txn: PersistentStoreTransaction) = OConcatenationIterator()

    inner class OConcatenationIterator() : NonDisposableEntityIterator(source) {
        private var iterator1: EntityIteratorBase? = this
        private var iterator2: EntityIteratorBase? = this

        override fun hasNextImpl(): Boolean {
            if (iterator1 === this) {
                iterator1 = this@OConcatEntityIterable.iterable1.iterator() as EntityIteratorBase
            }
            if (iterator1 != null) {
                if (iterator1!!.hasNext()) {
                    return true
                }
                iterator1 = null
                iterator2 = this@OConcatEntityIterable.iterable2.iterator() as EntityIteratorBase
            }
            if (iterator2 != null) {
                if (iterator2!!.hasNext()) {
                    return true
                }
                iterator2 = null
            }
            return false
        }

        public override fun nextIdImpl(): EntityId? {
            if (iterator1 != null) {
                return iterator1!!.nextId()
            }
            if (iterator2 != null) {
                return iterator2!!.nextId()
            }
            return null
        }
    }
}
