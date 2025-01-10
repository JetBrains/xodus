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

import com.jetbrains.youtrack.db.api.DatabaseType
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfigBuilder
import kotlin.math.min

class ODatabaseConfig private constructor(
    val connectionConfig: ODatabaseConnectionConfig,
    val databaseName: String,
    val databaseType: DatabaseType,
    val closeAfterDelayTimeout: Int,
    val cipherKey: ByteArray?,
    val closeDatabaseInDbProvider: Boolean,
    val tweakConfig: YouTrackDBConfigBuilder.() -> Unit
) {
    companion object {
        fun builder(): Builder {
            return Builder()
        }
    }

    @Suppress("unused")
    class Builder internal constructor() {
        private lateinit var connectionConfig: ODatabaseConnectionConfig
        private var databaseName: String = ""
        private var databaseType: DatabaseType? = null
        private var closeAfterDelayTimeout: Int? = null
        private var cipherKey: ByteArray? = null
        private var closeDatabaseInDbProvider = true
        private var tweakConfig: YouTrackDBConfigBuilder.() -> Unit = {}

        fun withDatabaseName(databaseName: String) = apply { this.databaseName = databaseName }
        fun withConnectionConfig(connectionConfig: ODatabaseConnectionConfig) =
            apply { this.connectionConfig = connectionConfig }

        fun withDatabaseType(databaseType: DatabaseType) = apply { this.databaseType = databaseType }
        fun withCloseAfterDelayTimeout(closeAfterDelayTimeout: Int) =
            apply { this.closeAfterDelayTimeout = closeAfterDelayTimeout }

        fun withCloseDatabaseInDbProvider(closeDatabaseInDbProvider: Boolean) =
            apply { this.closeDatabaseInDbProvider = closeDatabaseInDbProvider }

        fun withCipherKey(cipherKey: ByteArray?) = apply { this.cipherKey = cipherKey }
        fun withStringHexAndIV(key: String, IV: Long) = apply {
            require(cipherKey == null) { "Cipher is already initialized" }
            byteArrayOf()
            cipherKey = hexStringToByteArray(key.substring(0, min(16 * 2, key.length))) + longToByteArray(IV)
        }

        fun tweakConfig(tweakConfig: YouTrackDBConfigBuilder.() -> Unit) = apply { this.tweakConfig = tweakConfig }

        fun build() = ODatabaseConfig(
            connectionConfig, databaseName, databaseType ?: connectionConfig.databaseType, closeAfterDelayTimeout ?: connectionConfig.closeAfterDelayTimeout,
            cipherKey, closeDatabaseInDbProvider, tweakConfig
        )
    }
}

private fun hexStringToByteArray(hexString: String): ByteArray {
    require(hexString.length % 2 == 0) { "Hex string must have an even length" }

    return ByteArray(hexString.length / 2) { i ->
        val index = i * 2
        ((Character.digit(hexString[index], 16) shl 4) + Character.digit(hexString[index + 1], 16)).toByte()
    }
}

private fun longToByteArray(value: Long): ByteArray {
    return ByteArray(8) { i ->
        (value shr (i * 8) and 0xFF).toByte()
    }
}
