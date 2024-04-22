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
package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.command.OCommandOutputListener
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal
import com.orientechnologies.orient.core.db.ODatabaseType
import com.orientechnologies.orient.core.db.tool.ODatabaseExport
import com.orientechnologies.orient.core.db.tool.ODatabaseImport
import mu.KLogging
import java.io.File

class ODatabaseCompacter(
    private val dbProvider: ODatabaseProvider,
    private val databaseType: ODatabaseType,
    private val databaseName: String,
    private val userName: String,
    private val password: String,
) {
    companion object : KLogging()

    fun compactDatabase() {
        val databaseLocation = File(dbProvider.databaseLocation)
        val backupFile = File(databaseLocation, "temp${System.currentTimeMillis()}")
        val listener = OCommandOutputListener { iText -> logger.info("Compacting database: $iText") }

        val exporter = ODatabaseExport(
            dbProvider.acquireSession() as ODatabaseDocumentInternal,
            backupFile.outputStream(),
            listener
        )
        logger.info("Dumping database...")
        exporter.exportDatabase()
        logger.info("Dropping existing database...")
        dbProvider.database.drop(databaseName)
        dbProvider.database.create(databaseName, databaseType, userName, password, "admin")
        logger.info("Closing database connections")
        dbProvider.close()
        logger.info("Importing database from dump")
        val importer = ODatabaseImport(
            dbProvider.acquireSession() as ODatabaseDocumentInternal,
            backupFile.inputStream(),
            listener
        )
        importer.importDatabase()
        backupFile.delete()
    }
}
