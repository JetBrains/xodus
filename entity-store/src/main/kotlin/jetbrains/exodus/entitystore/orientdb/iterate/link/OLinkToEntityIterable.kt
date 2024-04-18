package jetbrains.exodus.entitystore.orientdb.iterate.link

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.OEntityId
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.*

class OLinkToEntityIterable(
    txn: StoreTransaction,
    private val entityType: String,
    private val linkName: String,
    private val linkEntityId: OEntityId,
) : OQueryEntityIterableBase(txn) {

    override fun query(): OSelect {
        return OClassSelect(entityType, OLinkEqualCondition(linkName, linkEntityId))
    }
}
