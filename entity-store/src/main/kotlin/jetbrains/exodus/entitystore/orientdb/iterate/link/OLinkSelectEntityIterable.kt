package jetbrains.exodus.entitystore.orientdb.iterate.link

import jetbrains.exodus.entitystore.PersistentStoreTransaction
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OLinkOutFromSubQuerySelect
import jetbrains.exodus.entitystore.orientdb.query.OSelect

class OLinkSelectEntityIterable(
    txn: PersistentStoreTransaction,
    private val source: OEntityIterableBase,
    private val linkName: String,
) : OEntityIterableBase(txn) {

    override fun query(): OSelect {
        return OLinkOutFromSubQuerySelect(linkName, source.query())
    }
}