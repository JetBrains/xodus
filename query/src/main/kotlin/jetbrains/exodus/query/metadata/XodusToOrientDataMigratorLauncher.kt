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
import java.io.File
import kotlin.time.measureTimedValue

private val log = KotlinLogging.logger { }

data class MigrateToOrientConfig(
    val databaseType: ODatabaseType,
    val url: String,
    val dbName: String,
    val username: String,
    val password: String,
)

data class MigrateFromXodusConfig(
    val databaseDirectory: String,
    val storeName: String,
    val cipherKey: String?,
    val cipherIV: Long
)

class XodusToOrientDataMigratorLauncher(
    val orient: MigrateToOrientConfig,
    val xodus: MigrateFromXodusConfig,
    val validateDataAfterMigration: Boolean,
) {
    fun migrate() {
        // 1. Where we migrate the data to

        // 1.1 Create ODatabaseProvider
        // create the database
        log.info { "1. Initialize OrientDB" }
        if (orient.databaseType != ODatabaseType.MEMORY) {
            val dir = File(orient.url.removePrefix("plocal:"))
            if (dir.exists()) {
                require(dir.list() != null) { "The provided OrientDB directory is not a directory. That is a bald move, man!" }
                require(dir.list()?.isEmpty() == true) { "The provided OrientDB directory is not empty. Sorry, pal, it was a good try. Try to find an empty directory." }
            }
        }
        val db = OrientDB(orient.url, OrientDBConfig.defaultConfig())
        if (orient.databaseType == ODatabaseType.MEMORY) {
            db.execute("create database ${orient.dbName} MEMORY users ( ${orient.username} identified by '${orient.password}' role admin )")
        } else {
            db.create(orient.dbName, orient.databaseType, orient.username, orient.password, "admin")
        }
        // create a provider
        val dbProvider = ODatabaseProviderImpl(db, orient.dbName, orient.username, orient.password, orient.databaseType)

        // 1.2 Create OModelMetadata
        // it is important to disable autoInitialize for the schemaBuddy,
        // dataMigrator does not like anything existing in the database before it migrated the data
        val schemaBuddy = OSchemaBuddyImpl(dbProvider, autoInitialize = false)
        val oModelMetadata = OModelMetaData(dbProvider, schemaBuddy)

        // 1.3 Create OPersistentEntityStore
        // it is important to pass the oModelMetadata to the entityStore as schemaBuddy.
        // it (oModelMetadata) must handle all the schema-related logic.
        val oEntityStore = OPersistentEntityStore(dbProvider, orient.dbName, schemaBuddy = oModelMetadata)

        // 1.4 Create TransientEntityStore
        // val oTransientEntityStore = TransientEntityStoreImpl(oModelMetadata, oEntityStore)


        // 2. Where we migrate the data from

        // 2.1 Create PersistentEntityStoreImpl
        log.info { "2. Open Xodus" }
        val env = Environments.newInstance(xodus.databaseDirectory, newEnvironmentConfig {
            isLogProceedDataRestoredAtAnyCost = true
            if (xodus.cipherKey != null) {
                setCipherKey(xodus.cipherKey)
                cipherBasicIV = xodus.cipherIV
            } else {
                require(cipherKey == null) { "Provided Xodus cipherKey is null, but there is not null cipherKey in the default config. Most probably, somebody again left a cipherKey in the gradle config. Delete it or nothing will work." }
            }
        })
        val xEntityStore = PersistentEntityStores.newInstance(env, xodus.storeName)

        try {
            xEntityStore.computeInTransaction { tx ->
                require(tx.entityTypes.size > 0) { "The Xodus database contains 0 entity types. Looks like a misconfiguration." }
            }

            // 3. Migrate the data
            val (migrateDataStats, migrateDataDuration) = measureTimedValue {
                migrateDataFromXodusToOrientDb(xEntityStore, oEntityStore)
            }
            schemaBuddy.initialize()

            // 4. Check data is the same
            val (checkDataStats, validateDataDuration) = measureTimedValue {
                if (validateDataAfterMigration) {
                    checkDataIsSame(xEntityStore, oEntityStore, migrateDataStats.xEntityIdToOEntityId)
                } else null
            }
            log.info { "Xodus -> OrientDB migration and validation completed" }
            with(migrateDataStats) {
                log.info {
                    """
                Data Migration stats
                    total duration: $migrateDataDuration
                        entity classes: $entityClasses
                        create entity classes duration: $createEntityClassesDuration ${percent(createEntityClassesDuration / migrateDataDuration)}
                        
                        entities: $entities
                        properties: $properties
                        blobs: $blobs
                        copy entities properties and blobs duration: $copyEntitiesPropertiesAndBlobsDuration ${percent(copyEntitiesPropertiesAndBlobsDuration / migrateDataDuration)}
                            create entities duration: $createEntitiesDuration ${percent(createEntitiesDuration / copyEntitiesPropertiesAndBlobsDuration)}
                            copy properties duration: $copyPropertiesDuration ${percent(copyPropertiesDuration / copyEntitiesPropertiesAndBlobsDuration)}
                            copy blobs duration: $copyBlobsDuration ${percent(copyBlobsDuration / copyEntitiesPropertiesAndBlobsDuration)}
                            commits duration: $commitEntitiesPropertiesAndBlobsDuration ${percent(commitEntitiesPropertiesAndBlobsDuration / copyEntitiesPropertiesAndBlobsDuration)}
                        
                        edge classes: $edgeClasses
                        create edge classes duration: $createEdgeClassesDuration ${percent(createEdgeClassesDuration / migrateDataDuration)}
                        
                        processed links: $processedLinks
                        copied links: $copiedLinks
                        copy links total duration: $copyLinksTotalDuration ${percent(copyLinksTotalDuration / migrateDataDuration)}
                            copy links duration: $copyLinksDuration ${percent(copyLinksDuration / copyLinksTotalDuration)}
                            commits duration: $commitLinksDuration ${percent(commitLinksDuration / copyLinksTotalDuration)}
            """.trimIndent()
                }
            }

            if (checkDataStats != null) {
                with(checkDataStats) {
                    log.info { """
                        Validation data stats
                            total duration: $validateDataDuration
                                check entity types duration: $checkEntityTypesDuration ${percent(checkEntityTypesDuration / validateDataDuration)}
                                
                                check entities duration: $checkEntitiesDuration ${percent(checkEntitiesDuration / validateDataDuration)}
                                    find entities duration:  $findEntitiesDuration ${percent(findEntitiesDuration / checkEntitiesDuration)}
                                    check properties duration: $checkPropertiesDuration ${percent(checkPropertiesDuration / checkEntitiesDuration)}
                                    check blobs duration: $checkBlobsDuration ${percent(checkBlobsDuration / checkEntitiesDuration)}
                                    check links duration: $checkLinksDuration ${percent(checkLinksDuration / checkEntitiesDuration)}
                    """.trimIndent() }
                }
            }
        } catch (e: Exception) {
            throw e
        } finally {
            // cleanup
            xEntityStore.close()
            db.close()
        }
    }
}

private fun percent(value: Double): String = "%.${2}f".format(value * 100) + "%"