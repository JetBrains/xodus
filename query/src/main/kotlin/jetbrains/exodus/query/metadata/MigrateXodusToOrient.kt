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
import jetbrains.exodus.entitystore.orientdb.ODatabaseConfig
import jetbrains.exodus.entitystore.orientdb.ODatabaseConnectionConfig
import jetbrains.exodus.entitystore.orientdb.ODatabaseProviderImpl
import jetbrains.exodus.entitystore.orientdb.iniYouTrackDb

fun main() {
    val xodusDatabaseDirectory = requireParam("xodusDatabaseDirectory")
    val xodusStoreName = requireParam("xodusStoreName")
    val xodusCipherKey = System.getProperty("xodusCipherKey")
    val xodusCipherIVStr = System.getProperty("xodusCipherIV")
    val xodusMemoryUsagePercentageStr = System.getProperty("xodusMemoryUsagePercentage")

    val orientDatabaseTypeStr = System.getProperty("orientDatabaseType")
    val orientDatabaseDirectoryStr = System.getProperty("orientDatabaseDirectory")
    val orientDatabaseNameStr = System.getProperty("orientDatabaseName")
    val orientUsernameStr = System.getProperty("orientUsername")
    val orientPasswordStr = System.getProperty("orientPassword")

    val validateDataAfterMigrationStr = System.getProperty("validateDataAfterMigration")
    val entitiesPerTransactionStr = System.getProperty("entitiesPerTransaction")
    println("""
            Provided params:
                xodusDatabaseDirectory: $xodusDatabaseDirectory
                xodusStoreName: $xodusStoreName
                xodusCipherKey: ${if (!xodusCipherKey.isNullOrBlank()) "provided" else "null" }
                xodusCipherIV: ${if (!xodusCipherKey.isNullOrBlank()) "provided" else "null" }
                xodusMemoryUsagePercentage: $xodusMemoryUsagePercentageStr
                
                orientDatabaseType: $orientDatabaseTypeStr
                orientDatabaseDirectory: $orientDatabaseDirectoryStr
                orientDatabaseName: $orientDatabaseNameStr
                orientUsername: $orientUsernameStr
                orientPassword: $orientPasswordStr
                
                validateDataAfterMigration: $validateDataAfterMigrationStr
                entitiesPerTransaction: $entitiesPerTransactionStr
        """.trimIndent())

    val xodusCypherIV = xodusCipherIVStr.toLongOrNull() ?: 0L
    val xodusMemoryUsagePercentage = xodusMemoryUsagePercentageStr.toIntOrNull() ?: 10
    val orientDatabaseType = if (orientDatabaseTypeStr.isNullOrBlank() || orientDatabaseTypeStr.lowercase() == "memory") {
        DatabaseType.MEMORY
    } else {
        DatabaseType.PLOCAL
    }
    val orientDatabaseDirectory = if (orientDatabaseType == DatabaseType.MEMORY) {
        "memory"
    } else {
        require(!orientDatabaseDirectoryStr.isNullOrBlank()) { "For not in-memory OrientDB, the orientDatabaseDirectory param is required" }
        if (orientDatabaseDirectoryStr.startsWith("plocal:")) orientDatabaseDirectoryStr else "plocal:$orientDatabaseDirectoryStr"
    }
    val orientDatabaseName = if (!orientDatabaseNameStr.isNullOrBlank()) orientDatabaseNameStr else "testDb"
    val orientUsername = if (!orientUsernameStr.isNullOrBlank()) orientUsernameStr else "admin"
    val orientPassword = if (!orientPasswordStr.isNullOrBlank()) orientPasswordStr else "password"

    val validateDataAfterMigration = validateDataAfterMigrationStr?.toBooleanStrictOrNull() ?: true
    val entitiesPerTransaction = validateDataAfterMigrationStr?.toIntOrNull() ?: 100

    println("""
            Effective params:
                xodusDatabaseDirectory: $xodusDatabaseDirectory
                xodusStoreName: $xodusStoreName
                xodusCipherKey: ${if (!xodusCipherKey.isNullOrBlank()) "provided" else "null" }
                xodusCipherIV: ${if (!xodusCipherKey.isNullOrBlank()) "provided" else "0" }
                xodusMemoryUsagePercentage: $xodusMemoryUsagePercentage
                
                orientDatabaseType: $orientDatabaseType
                orientDatabaseDirectory: $orientDatabaseDirectory
                orientDatabaseName: $orientDatabaseName
                orientUsername: $orientUsername
                orientPassword: $orientPassword
               
                validateDataAfterMigration: $validateDataAfterMigration
                entitiesPerTransaction: $entitiesPerTransaction
        """.trimIndent())

    val connectionConfig = ODatabaseConnectionConfig.builder()
        .withPassword(orientPassword)
        .withUserName(orientUsername)
        .withDatabaseRoot(orientDatabaseDirectory)
        .withDatabaseType(orientDatabaseType)
        .build()

    val config = ODatabaseConfig.builder()
        .withDatabaseName(orientDatabaseName)
        .build()

    val db = iniYouTrackDb(connectionConfig)
    // create a provider
    val dbProvider = ODatabaseProviderImpl(config, db)

    val launcher = XodusToOrientDataMigratorLauncher(
        xodus = MigrateFromXodusConfig(
            databaseDirectory = xodusDatabaseDirectory,
            storeName = xodusStoreName,
            cipherKey = xodusCipherKey,
            cipherIV = xodusCypherIV,
            memoryUsagePercentage = xodusMemoryUsagePercentage
        ),
        orient = MigrateToOrientConfig(
            databaseProvider = dbProvider,
            db = db,
            orientConfig = config,
            true
        ),
        validateDataAfterMigration = validateDataAfterMigration,
        entitiesPerTransaction = entitiesPerTransaction,
    )
    launcher.migrate()
}

private fun requireParam(name: String): String {
    val value = System.getProperty(name)
    require(!value.isNullOrBlank()) { "The required param '$name' is missing" }
    return value
}
