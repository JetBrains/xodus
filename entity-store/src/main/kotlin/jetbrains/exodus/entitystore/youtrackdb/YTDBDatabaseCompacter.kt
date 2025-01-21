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

import com.jetbrains.youtrack.db.api.YouTrackDB
import com.jetbrains.youtrack.db.internal.core.command.CommandOutputListener
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal
import com.jetbrains.youtrack.db.internal.core.db.tool.DatabaseExport
import com.jetbrains.youtrack.db.internal.core.db.tool.DatabaseImport
import mu.KLogging
import java.io.File

class YTDBDatabaseCompacter(
    private val db: YouTrackDB,
    private val dbProvider: YTDBDatabaseProvider,
    private val config: YTDBDatabaseConfig
) {
    companion object : KLogging()

    fun compactDatabase() {
        val databaseLocation = File(dbProvider.databaseLocation)
        val backupFile = File(databaseLocation, "temp${System.currentTimeMillis()}")
        backupFile.parentFile.mkdirs()
        val listener = CommandOutputListener { iText -> logger.info("Compacting database: $iText") }

        dbProvider.withSession { session ->
            val exporter = DatabaseExport(
                session as DatabaseSessionInternal,
                backupFile.outputStream(),
                listener
            )
            logger.info("Dumping database...")
            exporter.exportDatabase()
        }

        logger.info("Dropping existing database...")
        db.drop(config.databaseName)

        db.create(config.databaseName, config.databaseType,
            config.connectionConfig.userName, config.connectionConfig.password, "admin")

        dbProvider.withSession { session ->
            logger.info("Importing database from dump")
            val importer = DatabaseImport(
                session as DatabaseSessionInternal,
                backupFile.inputStream(),
                listener
            )
            importer.importDatabase()
        }
        backupFile.delete()
    }
}
