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

import com.orientechnologies.orient.core.db.ODatabaseType
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder
import kotlin.math.min

class ODatabaseConfig private constructor(
    val databaseRoot: String,
    val databaseName: String,
    val userName: String,
    val password: String,
    val databaseType: ODatabaseType,
    val closeAfterDelayTimeout: Int,
    val cipherKey: ByteArray?,
    val closeDatabaseInDbProvider: Boolean,
    val tweakConfig: OrientDBConfigBuilder.() -> Unit
) {
    companion object {
        fun builder(): Builder {
            return Builder()
        }
    }

    @Suppress("unused")
    class Builder internal constructor() {
        private var databaseName: String = ""
        private var databaseRoot: String = ""
        private var userName: String = ""
        private var password: String = ""
        private var databaseType: ODatabaseType = ODatabaseType.MEMORY
        private var closeAfterDelayTimeout: Int = 10
        private var cipherKey: ByteArray? = null
        private var closeDatabaseInDbProvider = true
        private var tweakConfig: OrientDBConfigBuilder.() -> Unit = {}

        fun withDatabaseName(databaseName: String) = apply { this.databaseName = databaseName }
        fun withDatabaseRoot(databaseURL: String) = apply { this.databaseRoot = databaseURL }
        fun withUserName(userName: String) = apply { this.userName = userName }
        fun withPassword(password: String) = apply { this.password = password }
        fun withDatabaseType(databaseType: ODatabaseType) = apply { this.databaseType = databaseType }
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

        fun tweakConfig(tweakConfig: OrientDBConfigBuilder.() -> Unit) = apply { this.tweakConfig = tweakConfig }

        fun build() = ODatabaseConfig(
            databaseRoot, databaseName, userName, password, databaseType,
            closeAfterDelayTimeout, cipherKey, closeDatabaseInDbProvider, tweakConfig
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
