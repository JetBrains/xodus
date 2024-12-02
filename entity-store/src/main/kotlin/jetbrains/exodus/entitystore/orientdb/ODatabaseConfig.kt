package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseType
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder

class ODatabaseConfig private constructor(
    val databaseRoot: String,
    val databaseName: String,
    val userName: String,
    val password: String,
    val databaseType: ODatabaseType,
    val closeAfterDelayTimeout: Int,
    val cipherKey: ByteArray?,
    val tweakConfig: OrientDBConfigBuilder.() -> Unit
) {
    companion object {
        fun builder(): Builder {
            return Builder()
        }
    }
    @Suppress("unused")
    class Builder internal constructor() {
        var databaseName: String = ""
        var databaseRoot: String = ""
        var userName: String = ""
        var password: String = ""
        var databaseType: ODatabaseType = ODatabaseType.MEMORY
        var closeAfterDelayTimeout: Int = 10
        var cipherKey: ByteArray? = null
        var tweakConfig: OrientDBConfigBuilder.() -> Unit = {}

        fun withDatabaseName(databaseName: String) = apply { this.databaseName = databaseName }
        fun withDatabaseRoot(databaseURL: String) = apply { this.databaseRoot = databaseURL }
        fun withUserName(userName: String) = apply { this.userName = userName }
        fun withPassword(password: String) = apply { this.password = password }
        fun withDatabaseType(databaseType: ODatabaseType) = apply { this.databaseType = databaseType }
        fun withCloseAfterDelayTimeout(closeAfterDelayTimeout: Int) =
            apply { this.closeAfterDelayTimeout = closeAfterDelayTimeout }

        fun withCipherKey(cipherKey: ByteArray?) = apply { this.cipherKey = cipherKey }
        fun withStringHexAndIV(key: String, IV: Long) = apply {
            require(cipherKey == null) { "Cipher is already initialized" }
            cipherKey = hexStringToByteArray(key) + longToByteArray(IV)
        }
        fun tweakConfig(tweakConfig: OrientDBConfigBuilder.() -> Unit) = apply { this.tweakConfig = tweakConfig }

        fun build() = ODatabaseConfig(
            databaseRoot, databaseName, userName, password, databaseType,
            closeAfterDelayTimeout, cipherKey, tweakConfig
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
