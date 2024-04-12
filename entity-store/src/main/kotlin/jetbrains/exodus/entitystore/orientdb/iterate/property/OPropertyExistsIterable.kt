package jetbrains.exodus.entitystore.iterate.property

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OClassSelect
import jetbrains.exodus.entitystore.orientdb.query.OFieldExistsCondition
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OPropertyExistsIterable(
    txn: StoreTransaction,
    private val entityType: String,
    private val propertyName: String,
) : OQueryEntityIterableBase(txn) {

    override fun query(): OSelect {
        return OClassSelect(entityType, OFieldExistsCondition(propertyName))
    }
}