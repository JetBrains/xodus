package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.sql.executor.OResultSet
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.PersistentEntityStore


fun OResultSet.toEntityIterator(store: PersistentEntityStore): Iterator<Entity> {
    return this.vertexStream().map { OVertexEntity(it, store) }.iterator()
}

fun ODatabaseSession.queryEntities(query: String, store: PersistentEntityStore): Iterable<Entity> {
    return Iterable {
        query(query).toEntityIterator(store)
    }
}
