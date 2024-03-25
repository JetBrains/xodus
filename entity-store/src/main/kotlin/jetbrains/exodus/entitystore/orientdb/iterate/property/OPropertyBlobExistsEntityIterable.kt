package jetbrains.exodus.entitystore.iterate.property

import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.DATA_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OAllSelect
import jetbrains.exodus.entitystore.orientdb.query.OFieldExistsCondition
import jetbrains.exodus.entitystore.orientdb.query.OQuery

class OPropertyBlobExistsEntityIterable(
    txn: PersistentStoreTransaction,
    private val className: String,
    private val blobName: String,
) : OEntityIterableBase(txn) {
    override fun query(): OQuery {
        return OAllSelect(className, OFieldExistsCondition("$blobName.${DATA_PROPERTY_NAME}"))
    }
}

