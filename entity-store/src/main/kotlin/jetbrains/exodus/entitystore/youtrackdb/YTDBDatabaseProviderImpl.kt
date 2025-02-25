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
package jetbrains.exodus.entitystore.youtrackdb

import com.jetbrains.youtrack.db.api.DatabaseSession
import com.jetbrains.youtrack.db.api.YouTrackDB
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig
import java.io.File

//username and password are considered to be same for all databases
//todo this params also should be collected in some config entity
class YTDBDatabaseProviderImpl(
    private val config: YTDBDatabaseConfig,
    private val database: YouTrackDB,
    private val dbConfig: YouTrackDBConfig = YouTrackDBConfigFactory.createDefaultDBConfig(config)
) : YTDBDatabaseProvider {
    override var isOpen: Boolean = false
        private set

    init {
        require(config.connectionConfig.userName.matches(Regex("^[a-zA-Z0-9]*$")))

        database.createIfNotExists(
            config.databaseName,
            config.databaseType,
            config.connectionConfig.userName,
            config.connectionConfig.password,
            "admin"
        )


        //todo migrate to some config entity instead of System props
        if (System.getProperty("exodus.env.compactOnOpen", "false").toBoolean()) {
            compact()
        }
        isOpen = true
    }

    fun compact() {
        YTDBDatabaseCompacter(
            database,
            this,
            config
        ).compactDatabase()
    }

    override val databaseLocation: String
        get() = File(config.connectionConfig.databaseRoot, config.databaseName).absolutePath

    override fun acquireSession(): DatabaseSession {
        return acquireSessionImpl(true)
    }

    override fun <T> executeInASeparateSession(
        currentSession: DatabaseSession,
        action: (DatabaseSession) -> T
    ): T {
        val result = try {
            acquireSessionImpl(checkNoActiveSession = false).use { session ->
                action(session)
            }
        } finally {
            // the previous session does not get activated on the current thread by default
            assert(!currentSession.isActiveOnCurrentThread)
            currentSession.activateOnCurrentThread()
        }
        return result
    }

    // it is always false at the beginning (it is impossible to close the database in the frozen state)
    private var _readOnly: Boolean = false

    override var readOnly: Boolean
        get() = _readOnly
        set(value) {
            if (_readOnly == value) return
            requireNoActiveSession()

            withSession { session ->
                if (value) {
                    // if one tries to write and commit changes, they will get an exception
                    session.freeze(true)
                } else {
                    session.release()
                }
            }
            _readOnly = value
        }

    private fun acquireSessionImpl(checkNoActiveSession: Boolean = true): DatabaseSession {
        if (checkNoActiveSession) {
            requireNoActiveSession()
        }
        return database.cachedPool(
            config.databaseName,
            config.connectionConfig.userName,
            config.connectionConfig.password,
            dbConfig
        ).acquire()
    }

    override fun close() {
        isOpen = false
        // OxygenDB cannot close the database if it is read-only (frozen)
        readOnly = false
        if (config.closeDatabaseInDbProvider) {
            database.close()
        }
    }
}
