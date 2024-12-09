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

import com.orientechnologies.orient.core.db.OrientDB
import jetbrains.exodus.entitystore.PersistentEntityStores
import jetbrains.exodus.entitystore.orientdb.*
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.newEnvironmentConfig
import jetbrains.exodus.log.BackupMetadata
import jetbrains.exodus.log.StartupMetadata
import mu.KotlinLogging
import org.apache.commons.io.FileUtils
import java.io.File
import kotlin.time.measureTimedValue

private val log = KotlinLogging.logger { }

val VERTEX_CLASSES_TO_SKIP_MIGRATION = 10


/**
 * Configuration for migrating to OrientDB from another system.
 *
 * @param databaseProvider Provides database connections.
 * @param db Instance of the OrientDB to which data is being migrated.
 * @param orientConfig Configuration settings specific to the OrientDB database.
 * @param closeOnFinish Flag indicating whether to close the database upon completion of migration.
 */
data class MigrateToOrientConfig(
    val databaseProvider: ODatabaseProvider,
    val db: OrientDB,
    val orientConfig: ODatabaseConfig,
    val closeOnFinish: Boolean = false
)

/**
 * Configuration for migrating from Xodus to another system.
 *
 * @param databaseDirectory The directory where the Xodus database is located.
 * @param storeName The name of the store within the Xodus database.
 * @param cipherKey Optional cipher key used for encrypting the database.
 * @param cipherIV The initialization vector used for encryption if a cipher key is provided.
 * @param memoryUsagePercentage Percentage of memory that can be used by the process.
 * @param dropOldDatabaseAfterMigration Flag indicating whether the old database should be deleted after migration.
 */
data class MigrateFromXodusConfig(
    val databaseDirectory: String,
    val storeName: String,
    val cipherKey: String?,
    val cipherIV: Long,
    val memoryUsagePercentage: Int,
    val dropOldDatabaseAfterMigration: Boolean = false
)

class XodusToOrientDataMigratorLauncher(
    val orient: MigrateToOrientConfig,
    val xodus: MigrateFromXodusConfig,
    val validateDataAfterMigration: Boolean,
    val entitiesPerTransaction: Int,
) {
    fun migrate() {
        // 0. Check if migration is already done
        // 0.0 No xodus directory or empty one -> no migration needed
        val xodusDir = File(xodus.databaseDirectory)
        if (!xodusDir.exists() || !xodusDir.isDirectory || xodusDir.list() == null || xodusDir.list()
                ?.isEmpty() == true || xodusDir.list { _, name -> name.endsWith(".xd", true) }
                ?.isEmpty() == true
        ) {
            log.info { "No xodus database found, migration not needed" }
            return
        }
        // 0.1 Orient database is already provided we can check already created tables in DB
        val dbProvider = orient.databaseProvider

        val dbName = orient.orientConfig.databaseName
        val classesCount = dbProvider.withSession {
            it.metadata.schema.classes.filter { !it.name.startsWith("O") }.size
        }
        if (classesCount > VERTEX_CLASSES_TO_SKIP_MIGRATION) {
            log.info { "There are already $classesCount classes in the database so it's considered as migrated" }
            return
        }


        // 2. Where we migrate the data from

        // 2.1 Create PersistentEntityStoreImpl
        log.info { "2. Open Xodus" }
        val env = Environments.newInstance(xodus.databaseDirectory, newEnvironmentConfig {
            isLogProceedDataRestoredAtAnyCost = true
            if (xodus.cipherKey != null) {
                setCipherId("jetbrains.exodus.crypto.streamciphers.JBChaChaStreamCipherProvider")
                setCipherKey(xodus.cipherKey)
                cipherBasicIV = xodus.cipherIV
            } else {
                require(cipherKey == null) { "Provided Xodus cipherKey is null, but there is not null cipherKey in the default config. Most probably, somebody again left a cipherKey in the gradle config. Delete it or nothing will work." }
            }
            memoryUsagePercentage = xodus.memoryUsagePercentage
        })
        val xEntityStore = PersistentEntityStores.newInstance(env, xodus.storeName)

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


        try {
            xEntityStore.computeInTransaction { tx ->
                require(tx.entityTypes.size > 0) { "The Xodus database contains 0 entity types. Looks like a misconfiguration." }
            }

            // 3. Migrate the data
            val (migrateDataStats, migrateDataDuration) = measureTimedValue {
                migrateDataFromXodusToOrientDb(
                    xEntityStore,
                    oEntityStore,
                    dbProvider,
                    oModelMetadata,
                    entitiesPerTransaction
                )
            }
            dbProvider.withSession {
                schemaBuddy.initialize(it)
            }

            // 4. Check data is the same
            val (checkDataStats, validateDataDuration) = measureTimedValue {
                if (validateDataAfterMigration) {
                    try {
                        checkDataIsSame(xEntityStore, oEntityStore, migrateDataStats.xEntityIdToOEntityId)
                    } catch (e: Exception) {
                        log.error(e) { "Error on the data checking after migration" }
                        null
                    }
                } else null
            }
            log.info { "Xodus -> OrientDB migration and validation completed" }
            with(migrateDataStats) {
                log.info {
                    """
                Data Migration stats
                    total duration: $migrateDataDuration
                        entity classes: $entityClasses
                        create entity classes duration: $createEntityClassesDuration ${
                        percent(
                            createEntityClassesDuration / migrateDataDuration
                        )
                    }
                        
                        entities: $entities
                        properties: $properties
                        blobs: $blobs
                        transactions: $copyEntitiesPropertiesAndBlobsTransactions
                        copy entities properties and blobs duration: $copyEntitiesPropertiesAndBlobsDuration ${
                        percent(
                            copyEntitiesPropertiesAndBlobsDuration / migrateDataDuration
                        )
                    }
                            create entities duration: $createEntitiesDuration ${percent(createEntitiesDuration / copyEntitiesPropertiesAndBlobsDuration)}
                            copy properties duration: $copyPropertiesDuration ${percent(copyPropertiesDuration / copyEntitiesPropertiesAndBlobsDuration)}
                            copy blobs duration: $copyBlobsDuration ${percent(copyBlobsDuration / copyEntitiesPropertiesAndBlobsDuration)}
                            commits duration: $commitEntitiesPropertiesAndBlobsDuration ${
                        percent(
                            commitEntitiesPropertiesAndBlobsDuration / copyEntitiesPropertiesAndBlobsDuration
                        )
                    }
                            single commit duration: ${commitEntitiesPropertiesAndBlobsDuration / copyEntitiesPropertiesAndBlobsTransactions.toInt()}

                        edge classes: $edgeClasses
                        create edge classes duration: $createEdgeClassesDuration ${percent(createEdgeClassesDuration / migrateDataDuration)}
                        
                        processed links: $processedLinks
                        copied links: $copiedLinks
                        transactions: $copyLinksTransactions
                        copy links total duration: $copyLinksTotalDuration ${percent(copyLinksTotalDuration / migrateDataDuration)}
                            copy links duration: $copyLinksDuration ${percent(copyLinksDuration / copyLinksTotalDuration)}
                            commits duration: $commitLinksDuration ${percent(commitLinksDuration / copyLinksTotalDuration)}
                            single commit duration: ${commitLinksDuration / copyLinksTransactions.toInt()}
            """.trimIndent()
                }
            }

            if (checkDataStats != null) {
                with(checkDataStats) {
                    log.info {
                        """
                        Validation data stats
                            total duration: $validateDataDuration
                                check entity types duration: $checkEntityTypesDuration ${
                            percent(
                                checkEntityTypesDuration / validateDataDuration
                            )
                        }
                                
                                check entities duration: $checkEntitiesDuration ${percent(checkEntitiesDuration / validateDataDuration)}
                                    find entities duration:  $findEntitiesDuration ${percent(findEntitiesDuration / checkEntitiesDuration)}
                                    check properties duration: $checkPropertiesDuration ${
                            percent(
                                checkPropertiesDuration / checkEntitiesDuration
                            )
                        }
                                    check blobs duration: $checkBlobsDuration ${percent(checkBlobsDuration / checkEntitiesDuration)}
                                    check links duration: $checkLinksDuration ${percent(checkLinksDuration / checkEntitiesDuration)}
                    """.trimIndent()
                    }
                }
            }
        } catch (e: Exception) {
            throw e
        } finally {
            // cleanup
            xEntityStore.close()
            cleanUpXodusDB(xodus.dropOldDatabaseAfterMigration)
            if (orient.closeOnFinish) {
                orient.db.close()
            }
        }
    }

    private fun cleanUpXodusDB(delete:Boolean) {
        val xodusSystemFilenames = hashSetOf(
            StartupMetadata.ZERO_FILE_NAME,
            StartupMetadata.FIRST_FILE_NAME,
            BackupMetadata.BACKUP_METADATA_FILE_NAME,
            BackupMetadata.START_BACKUP_METADATA_FILE_NAME,
            "xd.lck"
        )
        if (delete){
            log.info("Removing blobs...")
            val blobsDir = File(xodus.databaseDirectory, "blobs")
            if (blobsDir.exists()) {
                FileUtils.deleteDirectory(blobsDir)
            }
            log.info("Blobs removed")

            log.info("Removing text index...")
            val textIndex = File(xodus.databaseDirectory, "textindex")
            if (textIndex.exists()){
                FileUtils.deleteDirectory(textIndex)
            }
            log.info("Text index removed...")

            log.info("Cleanup xd files...")
            val dbDirectory = File(xodus.databaseDirectory)
            if (dbDirectory.exists() && dbDirectory.isDirectory){
                dbDirectory.listFiles { file ->
                    val name = file.name
                    name.endsWith(".xd") || xodusSystemFilenames.contains(name)
                }?.forEach {
                    it.delete()
                }
            }
            log.info("DB Files cleanup finished...")
        } else {
            log.info("Moving xodus files to backup directory")
            val newRoot = File(xodus.databaseDirectory, "xodus_backup")
            log.info("Moving blobs...")
            val blobsDir = File(xodus.databaseDirectory, "blobs")
            if (blobsDir.exists() && blobsDir.isDirectory){
                FileUtils.moveDirectory(blobsDir, File(newRoot, "blobs"))
            }
            log.info("Moving text-index...")
            val textIndex = File(xodus.databaseDirectory, "textindex")
            if (textIndex.exists()){
                FileUtils.moveDirectory(textIndex, File(newRoot, "textIndex"))
            }
            log.info("Moving xd files...")
            val dbDirectory = File(xodus.databaseDirectory)
            if (dbDirectory.exists() && dbDirectory.isDirectory){
                dbDirectory.listFiles { file ->
                    val name = file.name
                    name.endsWith(".xd") || xodusSystemFilenames.contains(name)
                }?.forEach {
                    FileUtils.moveFile(it, File(newRoot, it.name))
                }
            }
            log.info("DB files moved")
        }
    }
}

private fun percent(value: Double): String = "%.${2}f".format(value * 100) + "%"
