package jetbrains.exodus.entitystore.orientdb.iterate.link

import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.orientdb.OEntityId
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OLinkInFromIdsSelect
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OLinkToEntityIterable(
    txn: PersistentStoreTransaction,
    private val linkName: String,
    private val targetId: OEntityId,
) : OEntityIterableBase(txn) {

    override fun query(): OSelect {
        return OLinkInFromIdsSelect(linkName, listOf(targetId.asOId()))
    }
}
