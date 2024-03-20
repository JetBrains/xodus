package jetbrains.exodus.entitystore.iterate.link

import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.OAllSelect
import jetbrains.exodus.entitystore.orientdb.OHasBlobCondition
import jetbrains.exodus.entitystore.orientdb.OQuery

class OWithBlobEntityIterable(
    txn: PersistentStoreTransaction,
    private val className: String,
    private val blobName: String,
) : OEntityIterableBase(txn) {
    override fun query(): OQuery {
        return OAllSelect(className, OHasBlobCondition(blobName))
    }
}

