package jetbrains.exodus.entitystore.iterate.property

import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.OAllSelect
import jetbrains.exodus.entitystore.orientdb.OFieldExistsCondition
import jetbrains.exodus.entitystore.orientdb.OQuery

class OPropertyExistsIterable(
    txn: PersistentStoreTransaction,
    private val entityType: String,
    private val propertyName: String,
) : OEntityIterableBase(txn) {

    override fun query(): OQuery {
        return OAllSelect(entityType, OFieldExistsCondition(propertyName))
    }
}