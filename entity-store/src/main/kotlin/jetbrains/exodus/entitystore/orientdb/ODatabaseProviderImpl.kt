/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.config.OGlobalConfiguration
import com.orientechnologies.orient.core.db.*
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

    override val databaseLocation: String
        get() = database.accessInternal.basePath

    override fun acquireSession(): ODatabaseSession {
        requireNoActiveSession()
        return database.cachedPool(databaseName, userName, password).acquire()
    }

    override fun close() {
        //TODO this should call some other method, also to remove pool from database internal hashmap
        database.cachedPool(databaseName, userName, password, config).close()
    }
}
