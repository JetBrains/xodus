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
package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.db.ODatabaseType
import org.junit.Ignore
import org.junit.Test

class MigrateYourDatabaseTest {

    val yourStoreName = "yourStoreName"
    val yourXodusDatabaseFolder: String = "/path/to/a/database"
    val yourCipherKey: String? = null
    val yourCipherIV = 0L

    /**
     * It is not a test to test some functionality.
     * It is rather a convenient entry point for a developer to try migrating
     * a local database with minimal hassle.
     * So feel free to provide your parameters, comment @Ignore and run.
     */
    @Test
    @Ignore
    fun `migrate data from Xodus to OrientDB`() {
        val launcher = XodusToOrientDataMigratorLauncher(
            orient = MigrateToOrientConfig(
                databaseType = ODatabaseType.MEMORY,
                url = "memory",
                dbName = "testDB",
                username = "admin",
                password = "password"
            ),
            xodus = MigrateFromXodusConfig(
                databaseDirectory = yourXodusDatabaseFolder,
                storeName = yourStoreName,
                cipherKey = yourCipherKey,
                cipherIV = yourCipherIV
            ),
            validateDataAfterMigration = true
        )
        launcher.migrate()
    }
}