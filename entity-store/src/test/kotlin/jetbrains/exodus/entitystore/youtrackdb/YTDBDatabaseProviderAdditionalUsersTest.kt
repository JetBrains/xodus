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

import com.google.common.truth.Truth.assertThat
import com.jetbrains.youtrack.db.api.DatabaseType
import com.jetbrains.youtrack.db.api.YourTracks
import com.jetbrains.youtrack.db.api.exception.SecurityAccessException
import java.nio.file.Files
import kotlin.io.path.absolutePathString
import kotlin.test.Test
import kotlin.test.fail


class YTDBDatabaseProviderAdditionalUsersTest {

    @Test
    fun `database provider can be created with additional users`() {

        val dbPath = Files.createTempDirectory("YTDB_additional_users_test").absolutePathString()

        val additionalUsers1 = listOf(
            YTDBUser(name = "reader", password = "reader_pass", role = "reader"),
            YTDBUser(name = "writer", password = "writer_pass", role = "writer"),
            YTDBUser(name = "admin", password = "admin_pass", role = "admin")
        )

        val params = YTDBDatabaseParams.builder()
            .withDatabasePath(dbPath)
            .withDatabaseType(DatabaseType.DISK)
            .withDatabaseName("testDB")
            .withAppUser("user", "password")
            .withAdditionalUsers(additionalUsers1)

        val provider1 = YTDBDatabaseProviderFactory.createProvider(params.build())
        try {
            provider1.withSession { session ->
                session.schema.createVertexClass("Test")
                session.transaction { tx ->
                    repeat(100) {
                        tx.newVertex("Test").setProperty("initial", true)
                    }
                }
            }
        } finally {
            provider1.close()
        }

        checkUsersPermissions(dbPath, additionalUsers1, "1")

        val additionalUsers2 = listOf(
            YTDBUser(name = "reader2", password = "reader_pass2", role = "reader"),
            YTDBUser(name = "writer2", password = "writer_pass2", role = "writer"),
            YTDBUser(name = "admin2", password = "admin_pass2", role = "admin")
        )

        // creating a new DB provider with a new set of additional users.
        // this should create new users in the DB and not remove the old ones.
        YTDBDatabaseProviderFactory
            .createProvider(params.withAdditionalUsers(additionalUsers2).build())
            .close()

        checkUsersPermissions(dbPath, additionalUsers1 + additionalUsers2, "2")
    }

    private fun checkUsersPermissions(
        dbPath: String,
        additionalUsers: List<YTDBUser>,
        classSuffix: String
    ) {
        val youtrackdb = YourTracks.embedded(dbPath)
        try {
            additionalUsers.forEach { user ->
                youtrackdb.open("testDB", user.name, user.password).use { session ->
                    session.transaction { tx ->
                        assertThat(tx.query("select from Test where initial = true").entityStream()).hasSize(100)
                    }

                    try {
                        session.transaction { tx ->
                            tx.newVertex("Test")
                        }
                        if (user.role == "reader") {
                            fail("User '${user.name}' should not be able to create new entities")
                        }
                    } catch (_: SecurityAccessException) {
                        if (user.role != "reader") {
                            fail("User '${user.name}' should be able to create new entities")
                        }
                    }

                    try {
                        session.schema.createVertexClass("AnotherTest_${user.name}_$classSuffix")
                        if (user.role != "admin") {
                            fail("User '${user.name}' should not be able to create new classes")
                        }
                    } catch (_: SecurityAccessException) {
                        if (user.role == "admin") {
                            fail("User '${user.name}' should be able to create new classes")
                        }
                    }
                }
            }
        } finally {
            youtrackdb.close()
        }
    }
}