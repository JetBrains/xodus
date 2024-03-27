package jetbrains.exodus.entitystore.orientdb.iterate.link

import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.orientdb.OEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OLinkInFromSubQuerySelect
import jetbrains.exodus.entitystore.orientdb.query.OQueries
import jetbrains.exodus.entitystore.orientdb.query.OQuery

class OLinkSortEntityIterable(
    txn: PersistentStoreTransaction,
    private val entityName: String,
    private val linkOrder: OEntityIterable,
    private val sourceOrder: OEntityIterable,
    private val linkName: String,
) : OEntityIterableBase(txn) {

    override fun query(): OQuery {
        val linkQuery = OLinkInFromSubQuerySelect(entityName, linkName, linkOrder.query())
        return OQueries.intersect(linkQuery, sourceOrder.query())
    }
}