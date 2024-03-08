package jetbrains.exodus.entitystore.orientdb

import jetbrains.exodus.entitystore.EntityIterable

interface OEntityIterable : EntityIterable {

    fun query(): OQuery
}