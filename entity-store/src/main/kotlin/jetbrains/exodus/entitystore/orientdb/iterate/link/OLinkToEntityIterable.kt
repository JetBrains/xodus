package jetbrains.exodus.entitystore.orientdb.iterate.link

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.OEntityId
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OLinkInFromIdsSelect
import jetbrains.exodus.entitystore.orientdb.query.OQuery

class OLinkToEntityIterable(
    txn: StoreTransaction,
    private val className: String,
    private val linkName: String,
    private val targetId: OEntityId,
) : OEntityIterableBase(txn) {

    override fun query(): OQuery {
        return OLinkInFromIdsSelect(className, linkName, listOf(targetId.asOId()))
    }
}
