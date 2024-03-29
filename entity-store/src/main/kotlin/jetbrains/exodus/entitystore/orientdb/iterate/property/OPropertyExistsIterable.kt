package jetbrains.exodus.entitystore.iterate.property

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OAllSelect
import jetbrains.exodus.entitystore.orientdb.query.OFieldExistsCondition
import jetbrains.exodus.entitystore.orientdb.query.OQuery

class OPropertyExistsIterable(
    txn: StoreTransaction,
    private val entityType: String,
    private val propertyName: String,
) : OEntityIterableBase(txn) {

    override fun query(): OQuery {
        return OAllSelect(entityType, OFieldExistsCondition(propertyName))
    }
}