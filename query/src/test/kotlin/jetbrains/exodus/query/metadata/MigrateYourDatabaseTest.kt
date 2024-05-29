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
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import jetbrains.exodus.entitystore.PersistentEntityStores
import jetbrains.exodus.entitystore.orientdb.ODatabaseProviderImpl
import jetbrains.exodus.entitystore.orientdb.OPersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OSchemaBuddyImpl
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.newEnvironmentConfig
import mu.KotlinLogging
import org.junit.Test
import kotlin.test.assertTrue

private val log = KotlinLogging.logger { }

class MigrateYourDatabaseTest {

    val yourStoreName = "teamsysstore"
    val yourXodusDatabaseFolder: String = "/Users/Kirill.Vasilenko/Downloads/ytdemoeng-backup/youtrack"
    val yourCipherKey: String? = null
    val yourCipherIV = 0L

    @Test
    fun `migrate data from Xodus to OrientDB`() {

        // 1. Where we migrate the data to

        // 1.1 Create ODatabaseProvider
        // params you need to provide through configs
        val username = "admin"
        val password = "password"
        val dbName = "testDB"
        val url = "memory"
        // create the database
        val db = OrientDB(url, OrientDBConfig.defaultConfig())
        db.execute("create database $dbName MEMORY users ( $username identified by '$password' role admin )")
        // create a provider
        val dbProvider = ODatabaseProviderImpl(db, dbName, username, password, ODatabaseType.MEMORY)

        // 1.2 Create OModelMetadata
        // it is important to disable autoInitialize for the schemaBuddy,
        // dataMigrator does not like anything existing in the database before it migrated the data
        val schemaBuddy = OSchemaBuddyImpl(dbProvider, autoInitialize = false)
        val oModelMetadata = OModelMetaData(dbProvider, schemaBuddy)

        // 1.3 Create OPersistentEntityStore
        // it is important to pass the oModelMetadata to the entityStore as schemaBuddy.
        // it (oModelMetadata) must handle all the schema-related logic.
        val oEntityStore = OPersistentEntityStore(dbProvider, dbName, schemaBuddy = oModelMetadata)

        // 1.4 Create TransientEntityStore
        // val oTransientEntityStore = TransientEntityStoreImpl(oModelMetadata, oEntityStore)


        // 2. Where we migrate the data from

        // 2.1 Create PersistentEntityStoreImpl
        val env = Environments.newInstance(yourXodusDatabaseFolder, newEnvironmentConfig {
            isLogProceedDataRestoredAtAnyCost = true
            if (yourCipherKey != null) {
                setCipherKey(yourCipherKey)
                cipherBasicIV = yourCipherIV
            }
        })
        val xEntityStore = PersistentEntityStores.newInstance(env, yourStoreName)

        try {
            xEntityStore.computeInTransaction { tx ->
                assertTrue(tx.entityTypes.size > 0)
            }

            // 3. Migrate the data
            migrateDataFromXodusToOrientDb(xEntityStore, oEntityStore)
            schemaBuddy.initialize()

            // 4. Check data is the same
            checkDataIsSame(xEntityStore, oEntityStore)
        } catch (e: Exception) {
            throw e
        } finally {
            // cleanup
            xEntityStore.close()
            db.close()
        }
    }
}