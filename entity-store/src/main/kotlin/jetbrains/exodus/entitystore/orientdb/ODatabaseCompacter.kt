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
            dbProvider.databaseSession as ODatabaseDocumentInternal,
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
            dbProvider.databaseSession as ODatabaseDocumentInternal,
            backupFile.inputStream(),
            listener
        )
        importer.importDatabase()
        backupFile.delete()
    }
}
