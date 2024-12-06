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
import jetbrains.exodus.entitystore.orientdb.ODatabaseConfig
import jetbrains.exodus.entitystore.orientdb.ODatabaseProviderImpl
import jetbrains.exodus.entitystore.orientdb.initOrientDbServer
import org.junit.Test
import kotlin.test.Ignore

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

        val config = ODatabaseConfig.builder()
            .withPassword("password")
            .withDatabaseName("testDB")
            .withUserName("admin")
            .withDatabaseType(ODatabaseType.MEMORY)
            .withDatabaseRoot("")
            .build()

        val db = initOrientDbServer(config)
        val provider = ODatabaseProviderImpl(config, db)
        val launcher = XodusToOrientDataMigratorLauncher(
            orient = MigrateToOrientConfig(
                db = db,
                databaseProvider = provider,
                orientConfig = config,
                closeOnFinish = true
            ),
            xodus = MigrateFromXodusConfig(
                databaseDirectory = yourXodusDatabaseFolder,
                storeName = yourStoreName,
                cipherKey = yourCipherKey,
                cipherIV = yourCipherIV,
                memoryUsagePercentage = 10
            ),
            validateDataAfterMigration = true,
            entitiesPerTransaction = 100
        )
        launcher.migrate()
    }
}
