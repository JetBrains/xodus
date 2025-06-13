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
import com.jetbrains.youtrack.db.api.YouTrackDB
import com.jetbrains.youtrack.db.api.YourTracks
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl
import com.jetbrains.youtrack.db.internal.server.YouTrackDBServer
import com.jetbrains.youtrack.db.internal.server.network.protocol.binary.NetworkProtocolBinary
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.NetworkProtocolHttpDb
import com.jetbrains.youtrack.db.internal.tools.config.ServerConfiguration
import com.jetbrains.youtrack.db.internal.tools.config.ServerEntryConfiguration
import com.jetbrains.youtrack.db.internal.tools.config.ServerNetworkConfiguration
import com.jetbrains.youtrack.db.internal.tools.config.ServerNetworkListenerConfiguration
import com.jetbrains.youtrack.db.internal.tools.config.ServerNetworkProtocolConfiguration
import com.jetbrains.youtrack.db.internal.tools.config.ServerUserConfiguration
import jetbrains.exodus.entitystore.youtrackdb.YTDBDatabaseParams
import jetbrains.exodus.entitystore.youtrackdb.YTDBDatabaseProvider
import jetbrains.exodus.entitystore.youtrackdb.YTDBDatabaseProviderImpl
import org.apache.commons.lang.RandomStringUtils

object YouTrackDBFactory {

    fun createEmbedded(params: YTDBDatabaseParams): YouTrackDB {
        val config = params.youTrackDBConfig
        return YourTracks.embedded(params.databasePath, config).apply {
            (this as? YouTrackDBImpl)?.let {
                it.serverPassword = params.password
                it.serverUser = params.userName
            }
        }
    }
}

object YTDBDatabaseProviderFactory {

    fun createProvider(params: YTDBDatabaseParams): YTDBDatabaseProvider {

        val (youTrackDb, server) =
            if (params.serverParams == null) {
                Pair(YouTrackDBFactory.createEmbedded(params), null)
            } else params.serverParams.let {

                val serverConfig = ServerConfiguration()

                val rootUser = ServerUserConfiguration("root", RandomStringUtils.randomAscii(16), "*")
                val connectUser =
                    it.serverConnectUser?.let { user -> ServerUserConfiguration(user, it.serverConnectPassword, "*") }
                serverConfig.users = if (connectUser == null) arrayOf(rootUser, connectUser) else arrayOf(rootUser)
                serverConfig.network = ServerNetworkConfiguration().apply {
                    protocols = mutableListOf()
                    listeners = mutableListOf()
                }
                if (it.httpEnabled) {
                    serverConfig.network.apply {
                        protocols.add(
                            ServerNetworkProtocolConfiguration(
                                "http", NetworkProtocolHttpDb::class.qualifiedName
                            )
                        )
                        listeners.add(ServerNetworkListenerConfiguration().apply {
                            ipAddress = it.httpBindAddress
                            portRange = "${it.httpPortRange.first}-${it.httpPortRange.second}"
                            protocol = "http"
                        })
                    }
                }
                if (it.binaryEnabled) {
                    serverConfig.network.apply {
                        protocols.add(
                            ServerNetworkProtocolConfiguration(
                                "binary", NetworkProtocolBinary::class.qualifiedName
                            )
                        )
                        listeners.add(ServerNetworkListenerConfiguration().apply {
                            ipAddress = it.binaryBindAddress
                            portRange = "${it.binaryPortRange.first}-${it.binaryPortRange.second}"
                            protocol = "binary"
                        })
                    }
                }

                serverConfig.properties = arrayOf(
                    ServerEntryConfiguration("log.console.level", it.logConsoleLevel),
                    ServerEntryConfiguration("log.file.level", it.logFileLevel),
                    ServerEntryConfiguration("server.database.path", params.databasePath),
                )

                val server = YouTrackDBServer(false)
                server.startup(serverConfig)
                server.activate()
                Pair(server.context, server)
            }
        return YTDBDatabaseProviderImpl(params, youTrackDb, server)
    }

    fun createProvider(params: YTDBDatabaseParams, youTrackDB: YouTrackDB): YTDBDatabaseProvider {
        return YTDBDatabaseProviderImpl(params, youTrackDB, server = null)
    }
}