package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDbInternalAccessor.accessInternal

class ODatabaseProviderImpl(
    override val database: OrientDB,
    private val databaseName: String,
    private val userName: String,
    private val password: String
) : ODatabaseProvider {
    override val databaseLocation: String
        get() = database.accessInternal.basePath

    override fun acquireSession(): ODatabaseSession {
        return database.cachedPool(databaseName, userName, password).acquire()
    }

    override fun close() {
        //TODO this should call some other method, also to remove pool from database internal hashmap
        database.cachedPool(databaseName, userName, password).close()
    }
}
