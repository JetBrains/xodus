package jetbrains.exodus.entitystore.iterate.property

import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OClassSelect
import jetbrains.exodus.entitystore.orientdb.query.OFieldExistsCondition
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OPropertyExistsIterable(
    txn: PersistentStoreTransaction,
    private val entityType: String,
    private val propertyName: String,
) : OEntityIterableBase(txn) {

    override fun query(): OSelect {
        return OClassSelect(entityType, OFieldExistsCondition(propertyName))
    }
}