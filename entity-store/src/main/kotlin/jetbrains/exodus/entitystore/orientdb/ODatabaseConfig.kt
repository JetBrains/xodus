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
        fun tweakConfig(tweakConfig: OrientDBConfigBuilder.() -> Unit) = apply { this.tweakConfig = tweakConfig }

        fun build() = ODatabaseConfig(
            databaseRoot, databaseName, userName, password, databaseType,
            closeAfterDelayTimeout, cipherKey, tweakConfig
        )
    }
}
