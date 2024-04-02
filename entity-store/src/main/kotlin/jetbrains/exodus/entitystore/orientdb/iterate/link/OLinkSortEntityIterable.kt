package jetbrains.exodus.entitystore.orientdb.iterate.link

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.OEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OLinkInFromSubQuerySelect
import jetbrains.exodus.entitystore.orientdb.query.OQueryFunctions
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OLinkSortEntityIterable(
    txn: StoreTransaction,
    private val linkOrder: OEntityIterable,
    private val linkName: String,
    private val sourceOrder: OEntityIterable,
) : OEntityIterableBase(txn) {

    override fun query(): OSelect {
        val linkQuery = OLinkInFromSubQuerySelect(linkName, linkOrder.query())
        return OQueryFunctions.intersect(linkQuery, sourceOrder.query())
    }
}