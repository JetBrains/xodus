package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.sql.executor.OResultSet
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityIterator

fun OResultSet.toIteratorOfEntity(): Iterator<Entity> {
    return this.vertexStream().map { OEntity(it) }.iterator()
}

fun OResultSet.toEntityIterator(): EntityIterator {
    val iterator = this.vertexStream().map { OEntity(it) }.iterator()
    return OEntityIterator(iterator)
}


fun ODatabaseSession.queryEntity(query: String): Iterable<Entity> {
    return return Iterable<Entity> {
        query(query).toIteratorOfEntity()
    }
}

fun ODatabaseSession.queryEntity(query: String, params: Map<String, Any>): Iterable<Entity> {
    return Iterable<Entity> {
        query(query, params).toEntityIterator()
    }
}

class OIterableEntity(
    private val session: ODatabaseDocument,
    private val query: String,
    private val params: Map<String, *>
) : Iterable<Entity> {

    override fun iterator(): EntityIterator {
        return session.query(query, params).toEntityIterator()
    }
}

