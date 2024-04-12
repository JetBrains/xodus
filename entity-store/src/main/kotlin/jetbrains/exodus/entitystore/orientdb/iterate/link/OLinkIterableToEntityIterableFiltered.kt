package jetbrains.exodus.entitystore.orientdb.iterate.link

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.OQueryEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OIntersectSelect
import jetbrains.exodus.entitystore.orientdb.query.OLinkInFromSubQuerySelect
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OLinkIterableToEntityIterableFiltered(
    txn: StoreTransaction,
    private val linkIterable: OQueryEntityIterable,
    private val linkName: String,
    private val source: OQueryEntityIterable,
) : OQueryEntityIterableBase(txn) {

    override fun query(): OSelect {
        val byLinkSelect = OLinkInFromSubQuerySelect(linkName, linkIterable.query())
        return OIntersectSelect(source.query(), byLinkSelect)
    }
}
