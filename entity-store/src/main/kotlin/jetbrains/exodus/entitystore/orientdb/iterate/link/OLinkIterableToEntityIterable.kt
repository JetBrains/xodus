package jetbrains.exodus.entitystore.orientdb.iterate.link

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.OQueryEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OLinkInFromSubQuerySelect
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OLinkIterableToEntityIterable(
    txn: StoreTransaction,
    private val linkIterable: OQueryEntityIterable,
    private val linkName: String
) : OQueryEntityIterableBase(txn) {

    override fun query(): OSelect {
        return OLinkInFromSubQuerySelect(linkName, linkIterable.query())
    }
}
