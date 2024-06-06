package jetbrains.exodus.query.metadata

import com.orientechnologies.orient.core.db.ODatabaseType

fun main() {
    val xodusDatabaseDirectory = requireParam("xodusDatabaseDirectory")
    val xodusStoreName = requireParam("xodusStoreName")
    val xodusCipherKey = System.getProperty("xodusCipherKey")
    val xodusCipherIVStr = System.getProperty("xodusCipherIV")

    val orientDatabaseTypeStr = System.getProperty("orientDatabaseType")
    val orientDatabaseDirectoryStr = System.getProperty("orientDatabaseDirectory")
    val orientDatabaseNameStr = System.getProperty("orientDatabaseName")
    val orientUsernameStr = System.getProperty("orientUsername")
    val orientPasswordStr = System.getProperty("orientPassword")

    val validateDataAfterMigrationStr = System.getProperty("validateDataAfterMigration")
    println("""
            Provided params:
                xodusDatabaseDirectory: $xodusDatabaseDirectory
                xodusStoreName: $xodusStoreName
                xodusCipherKey: ${if (!xodusCipherKey.isNullOrBlank()) "provided" else "null" }
                xodusCipherIV: ${if (!xodusCipherKey.isNullOrBlank()) "provided" else "null" }
                
                orientDatabaseType: $orientDatabaseTypeStr
                orientDatabaseDirectory: $orientDatabaseDirectoryStr
                orientDatabaseName: $orientDatabaseNameStr
                orientUsername: $orientUsernameStr
                orientPassword: $orientPasswordStr
                
                validateDataAfterMigration: $validateDataAfterMigrationStr
        """.trimIndent())

    val xodusCypherIV = xodusCipherIVStr.toLongOrNull() ?: 0L
    val orientDatabaseType = if (orientDatabaseTypeStr.isNullOrBlank() || orientDatabaseTypeStr.lowercase() == "memory") {
        ODatabaseType.MEMORY
    } else {
        ODatabaseType.PLOCAL
    }
    val orientDatabaseDirectory = if (orientDatabaseType == ODatabaseType.MEMORY) {
        "memory"
    } else {
        require(!orientDatabaseDirectoryStr.isNullOrBlank()) { "For not in-memory OrientDB, the orientDatabaseDirectory param is required" }
        if (orientDatabaseDirectoryStr.startsWith("plocal:")) orientDatabaseDirectoryStr else "plocal:$orientDatabaseDirectoryStr"
    }
    val orientDatabaseName = if (!orientDatabaseNameStr.isNullOrBlank()) orientDatabaseNameStr else "testDb"
    val orientUsername = if (!orientUsernameStr.isNullOrBlank()) orientUsernameStr else "admin"
    val orientPassword = if (!orientPasswordStr.isNullOrBlank()) orientPasswordStr else "password"

    val validateDataAfterMigration = validateDataAfterMigrationStr?.toBooleanStrictOrNull() ?: true

    println("""
            Effective params:
                xodusDatabaseDirectory: $xodusDatabaseDirectory
                xodusStoreName: $xodusStoreName
                xodusCipherKey: ${if (!xodusCipherKey.isNullOrBlank()) "provided" else "null" }
                xodusCipherIV: ${if (!xodusCipherKey.isNullOrBlank()) "provided" else "0" }
                
                orientDatabaseType: $orientDatabaseType
                orientDatabaseDirectory: $orientDatabaseDirectory
                orientDatabaseName: $orientDatabaseName
                orientUsername: $orientUsername
                orientPassword: $orientPassword
               
                validateDataAfterMigration: $validateDataAfterMigration
        """.trimIndent())

    val launcher = XodusToOrientDataMigratorLauncher(
        xodus = MigrateFromXodusConfig(
            databaseDirectory = xodusDatabaseDirectory,
            storeName = xodusStoreName,
            cipherKey = xodusCipherKey,
            cipherIV = xodusCypherIV
        ),
        orient = MigrateToOrientConfig(
            databaseType = orientDatabaseType,
            url = orientDatabaseDirectory,
            dbName = orientDatabaseName,
            username = orientUsername,
            password = orientPassword
        ),
        validateDataAfterMigration = validateDataAfterMigration
    )
    launcher.migrate()
}

private fun requireParam(name: String): String {
    val value = System.getProperty(name)
    require(!value.isNullOrBlank()) { "The required param '$name' is missing" }
    return value
}
