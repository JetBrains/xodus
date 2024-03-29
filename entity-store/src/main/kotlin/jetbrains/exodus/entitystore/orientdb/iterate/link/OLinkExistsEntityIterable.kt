package jetbrains.exodus.entitystore.orientdb.iterate.link

import jetbrains.exodus.entitystore.StoreTransaction
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OAllSelect
import jetbrains.exodus.entitystore.orientdb.query.OEdgeExistsCondition
import jetbrains.exodus.entitystore.orientdb.query.OQuery

class OLinkExistsEntityIterable(
    txn: StoreTransaction,
    private val className: String,
    private val linkName: String,
) : OEntityIterableBase(txn) {

    override fun query(): OQuery {
        return OAllSelect(className, OEdgeExistsCondition(linkName))
    }
}
