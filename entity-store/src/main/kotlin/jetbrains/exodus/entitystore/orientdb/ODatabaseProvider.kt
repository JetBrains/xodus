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

import com.orientechnologies.orient.core.config.OGlobalConfiguration
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder
import kotlin.streams.asSequence

interface ODatabaseProvider {
    val databaseLocation: String
    fun acquireSession(): ODatabaseSession

    /**
     * If there is a session on the current thread, create a new session, executes the action in it,
     * and returns the previous session back to the current thread.
     *
     * Never use this method. If you use this method, make sure you 100% understand what happens,
     * and do not hesitate to invite people to review your code.
     */
    fun <T> executeInASeparateSession(
        currentSession: ODatabaseSession,
        action: (ODatabaseSession) -> T
    ): T

    /**
     * Database-wise read-only mode.
     * Always false by default.
     */
    var readOnly: Boolean

    fun close()
}

fun <R> ODatabaseProvider.withSession(block: (ODatabaseSession) -> R): R {
    acquireSession().use { session ->
        return block(session)
    }
}

fun <R> ODatabaseProvider.withCurrentOrNewSession(
    requireNoActiveTransaction: Boolean = false,
    block: (ODatabaseSession) -> R
): R {
    return if (hasActiveSession()) {
        val activeSession = ODatabaseSession.getActiveSession() as ODatabaseSession
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

internal fun ODatabaseSession.hasActiveTransaction(): Boolean {
    return isActiveOnCurrentThread && activeTxCount() > 0
}

internal fun ODatabaseSession.requireActiveTransaction() {
    require(hasActiveTransaction()) { "No active transaction is found. Happy debugging, pal!" }
}

internal fun ODatabaseSession.requireNoActiveTransaction() {
    assert(isActiveOnCurrentThread && activeTxCount() == 0) { "Active transaction is detected. Changes in the schema must not happen in a transaction." }
}

internal fun requireNoActiveSession() {
    check(!hasActiveSession()) { "Active session is detected on the current thread" }
}

internal fun hasActiveSession(): Boolean {
    val db = ODatabaseRecordThreadLocal.instance().getIfDefined()
    return db != null
}

fun initOrientDbServer(config: ODatabaseConfig): OrientDB {
    val orientConfig = OrientDBConfigBuilder().apply {
        addConfig(OGlobalConfiguration.AUTO_CLOSE_AFTER_DELAY, true)
        addConfig(OGlobalConfiguration.AUTO_CLOSE_DELAY, config.closeAfterDelayTimeout)
        addConfig(OGlobalConfiguration.NON_TX_READS_WARNING_MODE, "SILENT")
    }.build()
    val dbType = config.databaseType.name.lowercase()
    val db = OrientDB("$dbType:${config.databaseRoot}", orientConfig)
    try {
        db.execute("create system user admin identified by :pass role root", mapOf("pass" to config.password))
    } catch (_: com.orientechnologies.orient.core.storage.ORecordDuplicatedException) {
    }
    return db
}
