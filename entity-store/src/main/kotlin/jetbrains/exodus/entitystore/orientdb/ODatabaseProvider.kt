package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.OrientDB

interface ODatabaseProvider {
    val databaseSession: ODatabaseSession
    val databaseLocation: String
    val database: OrientDB
    fun close()
}
