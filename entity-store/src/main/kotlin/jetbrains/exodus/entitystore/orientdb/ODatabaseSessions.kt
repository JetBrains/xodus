package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import jetbrains.exodus.entitystore.Entity

fun ODatabaseSession.queryEntity(query: String): Iterable<Entity> {
    return return Iterable<Entity> {
        query(query).toEntityIterator()
    }
}