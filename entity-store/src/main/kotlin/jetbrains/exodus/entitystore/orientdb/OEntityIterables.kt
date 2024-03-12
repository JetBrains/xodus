package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.sql.executor.OResultSet
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.iterate.OEntityIterableBase


fun OResultSet.toEntityIterator(): Iterator<Entity> {
    return this.vertexStream().map { OVertexEntity(it) }.iterator()
}

fun OResultSet.toOEntityIterator(iterable: OEntityIterableBase): OEntityIterator {
    val iterator = this.toEntityIterator()
    return OEntityIterator(iterable, iterator)
}


fun ODatabaseSession.queryEntity(query: String): Iterable<Entity> {
    return return Iterable<Entity> {
        query(query).toEntityIterator()
    }
}

class OIterableEntity(
    private val session: ODatabaseDocument,
    private val query: String,
    private val params: Map<String, *>
) : Iterable<Entity> {

    override fun iterator(): Iterator<Entity> {
        return session.query(query, params).toEntityIterator()
    }
}

