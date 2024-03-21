package jetbrains.exodus.entitystore.orientdb

import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.orientdb.query.OQuery

interface OEntityIterable : EntityIterable {

    fun query(): OQuery
}