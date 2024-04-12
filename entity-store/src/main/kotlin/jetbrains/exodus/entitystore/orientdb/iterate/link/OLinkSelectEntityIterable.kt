package jetbrains.exodus.entitystore.orientdb.iterate.link

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.OQueryEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OLinkOutFromSubQuerySelect
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OLinkSelectEntityIterable(
    txn: StoreTransaction?,
    private val source: OQueryEntityIterable,
    private val linkName: String,
) : OQueryEntityIterableBase(txn) {

    override fun query(): OSelect {
        return OLinkOutFromSubQuerySelect(linkName, source.query())
    }
}