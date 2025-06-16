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
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityUserImpl
import com.jetbrains.youtrack.db.internal.server.YouTrackDBServer
import mu.KLogging
import java.io.File
import kotlin.streams.asSequence

//username and password are considered to be same for all databases
//todo this params also should be collected in some config entity
class YTDBDatabaseProviderImpl(
    private val params: YTDBDatabaseParams,
    private val database: YouTrackDB,
    private val server: YouTrackDBServer?
) : YTDBDatabaseProvider {

    override var isOpen: Boolean = false
        private set

    companion object : KLogging()

    init {
        val userNames = listOf(params.appUser.name) + params.additionalUsers.map { it.name }

        require(userNames.toSet().size == userNames.size) { "User names must be unique" }

        database.createIfNotExists(
            params.databaseName,
            params.databaseType,
            params.appUser.name,
            params.appUser.password,
            "admin"
        )

        // ToDo: migrate to some config entity instead of System props
        if (System.getProperty("exodus.env.compactOnOpen", "false").toBoolean()) {
            compact()
        }

        if (params.additionalUsers.any()) {
            acquireSessionImpl().use { session ->
                session.transaction { tx ->
                    val existingNames = tx.query("SELECT name FROM " + SecurityUserImpl.CLASS_NAME)
                        .stream()
                        .map { it.getString("name") }
                        .asSequence()
                        .toSet()

                    params.additionalUsers
                        .asSequence()
                        .filterNot { existingNames.contains(it.name) }
                        .forEach { userDef ->
                            logger.info { "Creating user ${userDef.name} with role ${userDef.role}" }

                            tx.command(
                                "CREATE USER ${userDef.name} IDENTIFIED BY :password ROLE ${userDef.role}",
                                mapOf("password" to userDef.password)
                            )
                        }
                }
            }
        }

        isOpen = true
    }

    fun compact() {
        YTDBDatabaseCompacter(database, this, params).compactDatabase()
    }

    override val databaseLocation: String
        get() = File(params.databasePath, params.databaseName).absolutePath

    override fun acquireSession(): DatabaseSession {
        return acquireSessionImpl()
    }

    // it is always false at the beginning (it is impossible to close the database in the frozen state)
    private var _readOnly: Boolean = false

    override var readOnly: Boolean
        get() = _readOnly
        set(value) {
            if (_readOnly == value) return

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

    private fun acquireSessionImpl(): DatabaseSession {
        return database.cachedPool(
            params.databaseName,
            params.appUser.name,
            params.appUser.password,
            params.youTrackDBConfig
        ).acquire()
    }

    override fun close() {
        isOpen = false
        // YouTrackDB cannot close the database if it is read-only (frozen)
        readOnly = false
        if (params.closeDatabaseInDbProvider) {
            if (server != null) {
                server.shutdown()
            } else {
                database.close()
            }
        }
    }
}
