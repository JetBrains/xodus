package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.config.OGlobalConfiguration
import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.ODatabaseType
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder
import com.orientechnologies.orient.core.db.OrientDbInternalAccessor.accessInternal

//todo this params also should be collected in some config entity
class ODatabaseProviderImpl(
    override val database: OrientDB,
    private val databaseName: String,
    private val userName: String,
    private val password: String,
    private val databaseType: ODatabaseType,
    private val closeAfterDelayTimeout: Int = 10,
) : ODatabaseProvider {

    private val config: OrientDBConfig

    init {
        config = OrientDBConfigBuilder().build().apply {
            configurations.setValue(OGlobalConfiguration.AUTO_CLOSE_AFTER_DELAY, true)
            configurations.setValue(OGlobalConfiguration.AUTO_CLOSE_DELAY, closeAfterDelayTimeout)
        }
        //todo migrate to some config entity instead of System props
        if (System.getProperty("exodus.env.compactOnOpen", "false").toBoolean()) {
            compact()
        }

    }

    fun compact() {
        ODatabaseCompacter(this, databaseType, databaseName, userName, password).compactDatabase()
    }

    override val databaseSession: ODatabaseSession
        get() {
            return database.cachedPool(databaseName, userName, password, config).acquire()
        }

    override val databaseLocation: String
        get() = database.accessInternal.basePath

    override fun close() {
        //TODO this should call some other method, also to remove pool from database internal hashmap
        database.cachedPool(databaseName, userName, password, config).close()
    }
}
