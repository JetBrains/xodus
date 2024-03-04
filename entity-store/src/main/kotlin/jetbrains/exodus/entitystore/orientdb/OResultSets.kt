package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.sql.executor.OResultSet
import jetbrains.exodus.entitystore.Entity

fun OResultSet.toEntityIterator(): Iterator<Entity> {
    return this.vertexStream().map { OrientDBEntity(it) }.iterator()
}
