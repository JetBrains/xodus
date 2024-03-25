package jetbrains.exodus.entitystore.orientdb.iterate.link

import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.orientdb.OEntityId
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OLinkInSelect
import jetbrains.exodus.entitystore.orientdb.query.OQuery

class OLinkToEntityIterable(
    txn: PersistentStoreTransaction,
    private val className: String,
    private val linkName: String,
    private val targetId: OEntityId,
) : OEntityIterableBase(txn) {

    override fun query(): OQuery {
        return OLinkInSelect(className, linkName, listOf(targetId.asOId()))
    }
}
