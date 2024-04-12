package jetbrains.exodus.entitystore.iterate.property

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.DATA_PROPERTY_NAME
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OClassSelect
import jetbrains.exodus.entitystore.orientdb.query.OFieldExistsCondition
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OPropertyBlobExistsEntityIterable(
    txn: StoreTransaction,
    private val className: String,
    private val blobName: String,
) : OQueryEntityIterableBase(txn) {

    override fun query(): OSelect {
        return OClassSelect(className, OFieldExistsCondition("$blobName.${DATA_PROPERTY_NAME}"))
    }
}

