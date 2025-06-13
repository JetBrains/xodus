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

import YTDBDatabaseProviderFactory
import com.google.common.truth.Truth.assertThat
import com.jetbrains.youtrack.db.api.DatabaseType
import com.jetbrains.youtrack.db.api.YourTracks
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig
import com.jetbrains.youtrack.db.api.exception.DatabaseException
import com.jetbrains.youtrack.db.api.remote.RemoteDatabaseSession
import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import kotlin.io.path.absolutePathString
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.fail

class YTDBServerTest {

    val username = "admin"
    val password = "admin_password"
    val serverConnectUser = "server_user"
    val serverConnectPassword = "server_password"
    val dbName = "testDB"
    lateinit var httpClient: HttpClient

    @BeforeTest
    fun setup() {
        httpClient = HttpClient()
    }

    @AfterTest
    fun tearDown() {
        httpClient.close()
    }

    @Test
    fun dbWithServerNew() {

        val dbPath = Files.createTempDirectory("YTDBServerTest_new").absolutePathString()

        val params = YTDBDatabaseParams.builder()
            .withDatabaseType(DatabaseType.DISK)
            .withDatabasePath(dbPath)
            .withPassword(password)
            .withUserName(username)
            .withDatabaseName(dbName)
            .withServerParams(
                YTDBServerParams(
                    serverConnectUser = serverConnectUser,
                    serverConnectPassword = serverConnectPassword,
                    httpEnabled = true,
                    binaryEnabled = true,
                )
            )
            .build()

        val db = YTDBDatabaseProviderFactory.createProvider(params)

        db.acquireSession().use { session ->
            session.schema.createVertexClass("Test")
            session.transaction { tx ->
                repeat(100) {
                    tx.newVertex("Test")
                }
            }
        }

        assertThat(remoteEntityCount("Test")).isEqualTo(100)
        assertThat(httpEntityCount("Test")).isEqualTo(100)
        db.close()

        checkServerDown()
    }

    @Test
    fun dbWithServerExisting() {

        val dbPath = Files.createTempDirectory("YTDBServerTest_existing").absolutePathString()

        val params = YTDBDatabaseParams.builder()
            .withDatabaseType(DatabaseType.DISK)
            .withDatabasePath(dbPath)
            .withPassword(password)
            .withUserName(username)
            .withDatabaseName(dbName)

        val dbNoServer = YTDBDatabaseProviderFactory.createProvider(params.build())
        dbNoServer.acquireSession().use { session ->
            session.schema.createVertexClass("Test")
            session.transaction { tx ->
                repeat(100) {
                    tx.newVertex("Test")
                }
            }
        }
        dbNoServer.close()

        val dbWithServer = YTDBDatabaseProviderFactory.createProvider(
            params
                .withServerParams(
                    YTDBServerParams(
                        serverConnectUser = serverConnectUser,
                        serverConnectPassword = serverConnectPassword,
                        httpEnabled = true,
                        binaryEnabled = true
                    )
                )
                .build()
        )
        assertThat(remoteEntityCount("Test")).isEqualTo(100)
        assertThat(httpEntityCount("Test")).isEqualTo(100)

        dbWithServer.acquireSession().use { session ->
            session.transaction { tx ->
                assertThat(tx.query("SELECT FROM Test").toList()).hasSize(100)
            }

            session.transaction { tx ->
                repeat(100) {
                    tx.newVertex("Test")
                }
            }
        }
        assertThat(remoteEntityCount("Test")).isEqualTo(200)
        assertThat(httpEntityCount("Test")).isEqualTo(200)
        dbWithServer.close()

        checkServerDown()
    }

    private fun checkServerDown() {
        try {
            remoteEntityCount("Test")
            fail("Server should be down")
        } catch (e: DatabaseException) {
            assertThat(e.message).contains("Cannot open database")
        }
    }

    private fun remoteSession(): RemoteDatabaseSession {
        return YourTracks
            .remote("remote:localhost", serverConnectUser, serverConnectPassword, YouTrackDBConfig.defaultConfig())
            .open(dbName, username, password)
    }

    private fun remoteEntityCount(className: String): Int =
        remoteSession().use { it.query("SELECT FROM $className").toList().size }

    private fun httpEntityCount(className: String): Int {
        val query = URLEncoder.encode("SELECT FROM $className", StandardCharsets.UTF_8)
        val response: String = runBlocking {
            httpClient
                .get("http://localhost:2480/query/$dbName/sql/$query/1000") {
                    basicAuth(username, password)
                }
                .bodyAsText()
        }

        return Json
            .parseToJsonElement(response).jsonObject
            .getValue("result").jsonArray
            .toList()
            .size
    }
}
