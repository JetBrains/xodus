package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.OrientDB

interface ODatabaseProvider {
    val databaseLocation: String
    val database: OrientDB
    fun acquireSession(): ODatabaseSession
    fun close()
}

fun <R> ODatabaseProvider.withSession(block: (ODatabaseSession) -> R): R {
    acquireSession().use { session ->
        return block(session)
    }
}
