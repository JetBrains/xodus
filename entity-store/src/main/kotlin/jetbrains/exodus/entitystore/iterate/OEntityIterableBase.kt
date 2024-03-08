package jetbrains.exodus.entitystore.iterate

import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.EntityIterableHandle
import jetbrains.exodus.entitystore.EntityIterator
import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.iterate.binop.OIntersectionIterable
import jetbrains.exodus.entitystore.iterate.binop.OUnionIterable
import jetbrains.exodus.entitystore.orientdb.OEntityIterable
import jetbrains.exodus.entitystore.orientdb.OEntityIterableHandle
import jetbrains.exodus.entitystore.orientdb.OEntityIterator

abstract class OEntityIterableBase(tx: PersistentStoreTransaction?) : EntityIterableBase(tx), OEntityIterable {

    override fun isSortedById() = false
    override fun canBeCached() = false

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIterator {
        val unionQuery = query()
        return OEntityIterator.create(this, txn.activeOSession(), unionQuery)
    }

    override fun getHandleImpl(): EntityIterableHandle {
        return OEntityIterableHandle(query().sql())
    }

    override fun union(right: EntityIterable): EntityIterable {
        if (right is OEntityIterableBase) {
            return OUnionIterable(transaction, this, right)
        }
        return super.union(right)
    }

    override fun intersect(right: EntityIterable): EntityIterable {
        if (right is OEntityIterableBase) {
            return OIntersectionIterable(transaction, this, right)
        }
        return super.intersect(right)
    }
}