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
import java.nio.file.Files
import java.nio.file.Path
import kotlin.time.measureTime
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
            val path = Path.of(orient.url)
            path.iterator()
            require(Files.notExists(path) || path.count() == 0) { "The provided OrientDB directory is not empty. Sorry, pal, it was a good try. Try to find an empty directory." }
        }
        val db = OrientDB(orient.url, OrientDBConfig.defaultConfig())
        db.execute("create database ${orient.dbName} ${if (orient.databaseType == ODatabaseType.MEMORY) "MEMORY" else ""} users ( ${orient.username} identified by '${orient.password}' role admin )")
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
            val validateDataDuration = measureTime {
                if (validateDataAfterMigration) {
                    checkDataIsSame(xEntityStore, oEntityStore)
                }
            }
            log.info { """
                Xodus -> OrientDB migration and validation completed
                Data Migration
                    total duration: $migrateDataDuration
                        entity classes: ${migrateDataStats.entityClasses}
                        create entity classes duration: ${migrateDataStats.createEntityClassesDuration}
                        
                        entities: ${migrateDataStats.entities}
                        properties: ${migrateDataStats.properties}
                        blobs: ${migrateDataStats.blobs}
                        copy entities properties and blobs duration: ${migrateDataStats.copyEntitiesPropertiesAndBlobsDuration}
                            create entities duration: ${migrateDataStats.createEntitiesDuration}
                            copy properties duration: ${migrateDataStats.copyPropertiesDuration}
                            copy blobs duration: ${migrateDataStats.copyBlobsDuration}
                            commits duration: ${migrateDataStats.commitEntitiesPropertiesAndBlobsDuration}
                        
                        edge classes: ${migrateDataStats.edgeClasses}
                        create edge classes duration: ${migrateDataStats.createEdgeClassesDuration}
                        
                        processed links: ${migrateDataStats.processedLinks}
                        copied links: ${migrateDataStats.copiedLinks}
                        copy links total duration: ${migrateDataStats.copyLinksTotalDuration}
                            copy links duration: ${migrateDataStats.copyLinksDuration}
                            commits duration: ${migrateDataStats.commitLinksDuration}
                        
                validateDataDuration: $validateDataDuration
            """.trimIndent() }
        } catch (e: Exception) {
            throw e
        } finally {
            // cleanup
            xEntityStore.close()
            db.close()
        }
    }
}