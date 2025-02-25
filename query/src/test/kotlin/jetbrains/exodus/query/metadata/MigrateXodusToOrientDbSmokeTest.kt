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

import com.jetbrains.youtrack.db.api.DatabaseType
import com.jetbrains.youtrack.db.api.record.Direction
import com.jetbrains.youtrack.db.api.record.Vertex
import jetbrains.exodus.TestUtil
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.entitystore.PersistentEntityStores
import jetbrains.exodus.entitystore.youtrackdb.*
import org.junit.Test
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MigrateXodusToOrientDbSmokeTest {

    @Test
    fun `migrate data and schema from Xodus to OrientDB`() {

        // 1. Where we migrate the data to

        // 1.1 Create ODatabaseProvider
        // params you need to provide through configs
        val username = "admin"
        val password = "password"
        val dbName = "testDB"
        val connectionConfig = YTDBDatabaseConnectionConfig.builder()
            .withPassword(password)
            .withUserName(username)
            .withDatabaseType(DatabaseType.MEMORY)
            .withDatabaseRoot("")
            .build()

        val config = YTDBDatabaseConfig.builder()
            .withConnectionConfig(connectionConfig)
            .withDatabaseName("MEMORY")
            .build()

        val dbConfig = YouTrackDBConfigFactory.createDefaultDBConfig(config)
        val db = YouTrackDBFactory.initYouTrackDb(config, dbConfig)
        db.execute("create database $dbName MEMORY users ( $username identified by '$password' role admin )")
        // create a provider
        val dbProvider = YTDBDatabaseProviderFactory.createProvider(config, db, dbConfig)

        // 1.2 Create OModelMetadata
        // it is important to disable autoInitialize for the schemaBuddy,
        // dataMigrator does not like anything existing in the database before it migrated the data
        val oModelMetadata = YTDBModelMetaData(dbProvider, YTDBSchemaBuddyImpl(dbProvider, autoInitialize = false))

        // 1.3 Create OPersistentEntityStore
        // it is important to pass the oModelMetadata to the entityStore as schemaBuddy.
        // it (oModelMetadata) must handle all the schema-related logic.
        val oEntityStore = YTDBPersistentEntityStore(dbProvider, dbName, schemaBuddy = oModelMetadata)

        // 1.4 Create TransientEntityStore
        // val oTransientEntityStore = TransientEntityStoreImpl(oModelMetadata, oEntityStore)


        // 2. Where we migrate the data from

        // 2.1 Create PersistentEntityStoreImpl
        val fromDatabaseFolder = File(TestUtil.createTempDir().absolutePath)
        val xEntityStore = PersistentEntityStores.newInstance(fromDatabaseFolder)
        // for the test purposes
        val entities = xEntityStore.createTestData()


        // 3. Migrate the data
        migrateDataFromXodusToOrientDb(xEntityStore, oEntityStore, dbProvider, oModelMetadata,)


        // 4. Initialize the schema
        // initMetaData(XdModel.hierarchy, oTransientEntityStore)
        // we do not have here neither TransientEntityStore nor initMetaData(...),
        // so we will apply the schema directly
        oModelMetadata.createTestSchema()
        oModelMetadata.prepare()

        // quick validations
        oEntityStore.checkContainsAllTheEntities(entities)
        oEntityStore.checkLinksWork()
        dbProvider.checkSchemaCreated()

        // cleanup
        xEntityStore.close()
        fromDatabaseFolder.deleteRecursively()
        db.close()
    }

    private fun YTDBPersistentEntityStore.checkContainsAllTheEntities(entities: PileOfEntities) {
        executeInTransaction { tx ->
            tx.assertOrientContainsAllTheEntities(entities)
        }
    }

    private fun YTDBDatabaseProvider.checkSchemaCreated() {
        withSession { session ->
            val type1 = session.getClass("type1")!!
            val type2 = session.getClass("type2")!!

            assertTrue(type1.existsProperty("prop4"))
            assertTrue(type1.existsProperty(Vertex.getEdgeLinkFieldName(Direction.IN, "link1".asEdgeClass)))
            assertTrue(type1.existsProperty(Vertex.getEdgeLinkFieldName(Direction.OUT, "link1".asEdgeClass)))

            assertTrue(type2.existsProperty("pop4"))
            assertTrue(type2.existsProperty(Vertex.getEdgeLinkFieldName(Direction.OUT, "link1".asEdgeClass)))
        }
    }

    private fun YTDBPersistentEntityStore.checkLinksWork() {
        executeInTransaction { tx ->
            for (entity in tx.getAll("type2")) {
                if (entity.getProperty("id") == 7) {
                    val targetEntity = entity.getLink("link1")!!

                    assertEquals(6, targetEntity.getProperty("id"))
                    assertEquals("type1", targetEntity.type)
                }
            }
            for (entity in tx.getAll("type1")) {
                if (entity.getProperty("id") == 6) {
                    val targetEntity = entity.getLink("link1")!!

                    assertEquals(2, targetEntity.getProperty("id"))
                    assertEquals("type1", targetEntity.type)
                }
            }
        }
    }

    private fun YTDBModelMetaData.createTestSchema() {
        entity("type1") {
            property("prop1", "string")
            property("prop2", "boolean")
            property("prop3", "double")
            property("prop4", "string")
        }
        entity("type2") {
            property("pop1", "string")
            property("pop2", "boolean")
            property("pop3", "double")
            property("pop4", "string")
        }

        association("type1","link1", "type1", AssociationEndCardinality._0_n)
        association("type2","link1", "type1", AssociationEndCardinality._0_n)
    }

    private fun PersistentEntityStoreImpl.createTestData(): PileOfEntities {
        val entities = pileOfEntities(
            eProps("type1", 1, "prop1" to "one", "prop2" to true, "prop3" to 1.1),
            eProps("type1", 2, "prop1" to "two", "prop2" to true, "prop3" to 2.2),
            eProps("type1", 3, "prop1" to "three", "prop2" to true, "prop3" to 3.3),

            eProps("type2", 2, "pop1" to "four", "pop2" to true, "pop3" to 2.2),
            eProps("type2", 4, "pop1" to "five", "pop2" to true, "pop3" to 3.3),
            eProps("type2", 5, "pop1" to "six", "pop2" to true, "pop3" to 4.4),

            eLinks("type1", 6,
                Link("link1", "type1", 2)
            ),
            eLinks("type2", 7,
                Link("link1", "type1", 6),
            )
        )
        computeInTransaction { tx ->
            tx.createEntities(entities)
        }
        return entities
    }
}
