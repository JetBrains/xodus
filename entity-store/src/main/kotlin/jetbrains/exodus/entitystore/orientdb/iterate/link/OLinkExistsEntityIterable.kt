package jetbrains.exodus.entitystore.orientdb.iterate.link

import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OClassSelect
import jetbrains.exodus.entitystore.orientdb.query.OEdgeExistsCondition
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OLinkExistsEntityIterable(
    txn: PersistentStoreTransaction,
    private val className: String,
    private val linkName: String,
) : OEntityIterableBase(txn) {

    override fun query(): OSelect {
        return OClassSelect(className, OEdgeExistsCondition(linkName))
    }
}
