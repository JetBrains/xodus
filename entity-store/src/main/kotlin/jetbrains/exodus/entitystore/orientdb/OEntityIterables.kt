package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.sql.executor.OResultSet
import jetbrains.exodus.entitystore.Entity


fun OResultSet.toEntityIterator(): Iterator<Entity> {
    return this.vertexStream().map { OVertexEntity(it) }.iterator()
}

fun ODatabaseSession.queryEntities(query: String): Iterable<Entity> {
    return return Iterable<Entity> {
        query(query).toEntityIterator()
    }
}
