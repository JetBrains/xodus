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

import com.jetbrains.youtrack.db.api.DatabaseType
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfigBuilder
import java.util.*
import kotlin.math.min

class YTDBDatabaseParams private constructor(
    val databasePath: String,
    val databaseName: String,
    val databaseType: DatabaseType,
    val userName: String,
    val password: String,
    val encryptionKey: String?,
    val closeDatabaseInDbProvider: Boolean,
    val closeAfterDelayTimeout: Int,
    val configBuilder: YouTrackDBConfigBuilder.() -> Unit = {}
) {

    companion object {

        fun builder(): Builder {
            return Builder()
        }
    }

    val youTrackDBConfig: YouTrackDBConfig = YouTrackDBConfig.builder()
        .addGlobalConfigurationParameter(GlobalConfiguration.AUTO_CLOSE_AFTER_DELAY, true)
        .addGlobalConfigurationParameter(GlobalConfiguration.AUTO_CLOSE_DELAY, closeAfterDelayTimeout)
        .apply {
            encryptionKey?.let { addGlobalConfigurationParameter(GlobalConfiguration.STORAGE_ENCRYPTION_KEY, it) }
        }
        .apply(configBuilder)
        .build()

    @Suppress("unused")
    class Builder internal constructor() {

        private var databasePath: String = ""
        private var databaseName: String = ""
        private var databaseType: DatabaseType = DatabaseType.MEMORY
        private var userName: String = "admin"
        private var password: String = "admin"
        private var closeAfterDelayTimeout: Int = 10
        private var encryptionKey: String? = null
        private var closeDatabaseInDbProvider = true
        private var configBuilder: YouTrackDBConfigBuilder.() -> Unit = {}

        fun withDatabasePath(databaseUrl: String) = apply {
            this.databasePath = databaseUrl
        }

        fun withDatabaseName(databaseName: String) = apply {
            this.databaseName = databaseName
        }

        fun withDatabaseType(databaseType: DatabaseType) = apply {
            this.databaseType = databaseType
        }

        fun withUserName(userName: String) = apply {
            this.userName = userName
        }

        fun withPassword(password: String) = apply {
            this.password = password
        }

        fun withCloseDatabaseInDbProvider(closeDatabaseInDbProvider: Boolean) = apply {
            this.closeDatabaseInDbProvider = closeDatabaseInDbProvider
        }

        fun withCloseAfterDelayTimeout(closeAfterDelayTimeout: Int) = apply {
            this.closeAfterDelayTimeout = closeAfterDelayTimeout
        }

        fun withEncryptionKey(encryptionKey: ByteArray) = apply {
            this.encryptionKey = Base64.getEncoder().encodeToString(encryptionKey)
        }

        fun withEncryptionKey(encryptionKey: String) = apply {
            this.encryptionKey = encryptionKey
        }

        fun withHexEncryptionKey(key: String, iv: Long) = apply {
            require(encryptionKey == null) { "Cipher is already initialized" }
            // Truncate the key to 16 bytes (32 hex symbols = 16 bytes) according to the YouTrackDB requirements
            val truncatedHex = key.substring(0, min(32, key.length))
            // 16 bytes hex + 8 bytes long iv = 24 bytes
            val bytes = HexFormat.of().parseHex(truncatedHex) + iv.toByteArray()
            withEncryptionKey(bytes)
        }

        fun withConfigBuilder(tweakConfig: YouTrackDBConfigBuilder.() -> Unit) = apply {
            this.configBuilder = tweakConfig
        }

        fun build(): YTDBDatabaseParams {
            return YTDBDatabaseParams(
                databasePath,
                databaseName,
                databaseType,
                userName,
                password,
                encryptionKey,
                closeDatabaseInDbProvider,
                closeAfterDelayTimeout,
                configBuilder
            )
        }

        private fun Long.toByteArray(): ByteArray {
            return ByteArray(8) { i ->
                (this shr (i * 8) and 0xFF).toByte()
            }
        }
    }
}


