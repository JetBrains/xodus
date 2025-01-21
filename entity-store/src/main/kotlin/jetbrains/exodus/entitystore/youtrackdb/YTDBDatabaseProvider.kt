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

import com.jetbrains.youtrack.db.api.DatabaseSession
import com.jetbrains.youtrack.db.api.YouTrackDB
import com.jetbrains.youtrack.db.api.YourTracks
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig
import com.jetbrains.youtrack.db.api.exception.RecordDuplicatedException
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal

interface YTDBDatabaseProvider {
    val databaseLocation: String
    fun acquireSession(): DatabaseSession

    /**
     * If there is a session on the current thread, create a new session, executes the action in it,
     * and returns the previous session back to the current thread.
     *
     * Never use this method. If you use this method, make sure you 100% understand what happens,
     * and do not hesitate to invite people to review your code.
     */
    fun <T> executeInASeparateSession(
        currentSession: DatabaseSession,
        action: (DatabaseSession) -> T
    ): T

    /**
     * Database-wise read-only mode.
     * Always false by default.
     */
    var readOnly: Boolean

    fun close()
}

fun <R> YTDBDatabaseProvider.withSession(block: (DatabaseSession) -> R): R {
    acquireSession().use { session ->
        return block(session)
    }
}

fun <R> YTDBDatabaseProvider.withCurrentOrNewSession(
    requireNoActiveTransaction: Boolean = false,
    block: (DatabaseSession) -> R
): R {
    return if (hasActiveSession()) {
        val activeSession = DatabaseRecordThreadLocal.instance().getIfDefined() as DatabaseSession
        if (requireNoActiveTransaction) {
            activeSession.requireNoActiveTransaction()
        }
        block(activeSession)
    } else {
        withSession { newSession ->
            block(newSession)
        }
    }
}

internal fun DatabaseSession.hasActiveTransaction(): Boolean {
    return isActiveOnCurrentThread && activeTxCount() > 0
}

internal fun DatabaseSession.requireActiveTransaction() {
    require(hasActiveTransaction()) { "No active transaction is found. Happy debugging, pal!" }
}

internal fun DatabaseSession.requireNoActiveTransaction() {
    assert(isActiveOnCurrentThread && activeTxCount() == 0) { "Active transaction is detected. Changes in the schema must not happen in a transaction." }
}

internal fun requireNoActiveSession() {
    check(!hasActiveSession()) { "Active session is detected on the current thread" }
}

internal fun hasActiveSession(): Boolean {
    val db = DatabaseRecordThreadLocal.instance().getIfDefined()
    return db != null
}

fun iniYouTrackDb(config: YTDBDatabaseConnectionConfig): YouTrackDB {
    val orientConfig = YouTrackDBConfig.builder().apply {
        addGlobalConfigurationParameter(GlobalConfiguration.AUTO_CLOSE_AFTER_DELAY, true)
        addGlobalConfigurationParameter(
            GlobalConfiguration.AUTO_CLOSE_DELAY,
            config.closeAfterDelayTimeout
        )
        addGlobalConfigurationParameter(GlobalConfiguration.NON_TX_READS_WARNING_MODE, "SILENT")
    }.build()
    require(config.userName.matches(Regex("^[a-zA-Z0-9]*$")))
    val db = YourTracks.embedded(config.databaseRoot, orientConfig)

    try {
        db.execute("create system user ${config.userName} identified by :pass role root", mapOf(
            "pass" to config.password,
        ))
    } catch (_: RecordDuplicatedException) {
    }

    return db
}
