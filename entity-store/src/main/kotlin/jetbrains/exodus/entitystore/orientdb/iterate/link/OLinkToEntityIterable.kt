package jetbrains.exodus.entitystore.orientdb.iterate.link

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.OEntityId
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OLinkInFromIdsSelect
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OLinkToEntityIterable(
    txn: StoreTransaction,
    private val linkName: String,
    private val linkEntityId: OEntityId,
) : OQueryEntityIterableBase(txn) {

    override fun query(): OSelect {
        return OLinkInFromIdsSelect(linkName, listOf(linkEntityId.asOId()))
    }
}
