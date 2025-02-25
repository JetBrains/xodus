import com.jetbrains.youtrack.db.api.YouTrackDB
import com.jetbrains.youtrack.db.api.YourTracks
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl
import jetbrains.exodus.entitystore.youtrackdb.YTDBDatabaseConfig
import jetbrains.exodus.entitystore.youtrackdb.YTDBDatabaseProvider
import jetbrains.exodus.entitystore.youtrackdb.YTDBDatabaseProviderImpl
import java.util.*

object YouTrackDBConfigFactory {

    fun createDefaultDBConfig(params: YTDBDatabaseConfig): YouTrackDBConfig {
        return YouTrackDBConfig.builder()
            .addGlobalConfigurationParameter(GlobalConfiguration.AUTO_CLOSE_AFTER_DELAY, true)
            .addGlobalConfigurationParameter(GlobalConfiguration.AUTO_CLOSE_DELAY, params.closeAfterDelayTimeout)
            .addGlobalConfigurationParameter(GlobalConfiguration.NON_TX_READS_WARNING_MODE, "SILENT")
            .apply {
                params.cipherKey?.also { key ->
                    addGlobalConfigurationParameter(
                        GlobalConfiguration.STORAGE_ENCRYPTION_KEY,
                        Base64.getEncoder().encodeToString(key)
                    )
                }
            }
            .apply(params.tweakConfig)
            .build()
    }
}

object YouTrackDBFactory {

    fun initYouTrackDb(params: YTDBDatabaseConfig, dbConfig: YouTrackDBConfig): YouTrackDB {
        return YourTracks.embedded(params.connectionConfig.databaseRoot, dbConfig).apply {
            (this as? YouTrackDBImpl)?.let {
                it.serverPassword = params.connectionConfig.password
                it.serverUser = params.connectionConfig.userName
            }
        }
    }
}

object YTDBDatabaseProviderFactory {

    fun createProviderWithDb(params: YTDBDatabaseConfig): YTDBDatabaseProvider {
        val dbConfig = YouTrackDBConfigFactory.createDefaultDBConfig(params)
        val youTrackDb = YouTrackDBFactory.initYouTrackDb(params, dbConfig)
        return createProvider(params, youTrackDb, dbConfig)
    }

    fun createProvider(params: YTDBDatabaseConfig, db: YouTrackDB, dbConfig: YouTrackDBConfig): YTDBDatabaseProvider {
        return YTDBDatabaseProviderImpl(params, db, dbConfig)
    }
}