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

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.tx.OTransaction
import com.orientechnologies.orient.core.tx.OTransactionNoTx

interface ODatabaseProvider {
    val databaseLocation: String
    val database: OrientDB
    fun acquireSession(): ODatabaseSession

    /**
     * If there is a session on the current thread, create a new session, executes the action in it,
     * and returns the previous session back to the current thread.
     *
     * Never use this method. If you use this method, make sure you 100% understand what happens,
     * and do not hesitate to invite people to review your code.
     */
    fun <T> executeInASeparateSession(currentSession: ODatabaseSession, action: (ODatabaseSession) -> T): T

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
    return isActiveOnCurrentThread && transaction !is OTransactionNoTx
}

internal fun ODatabaseSession.requireActiveTransaction(): OTransaction {
    require(hasActiveTransaction()) { "No active transaction is found. Happy debugging, pal!" }
    return transaction
}

internal fun ODatabaseSession.requireNoActiveTransaction() {
    assert(isActiveOnCurrentThread && transaction is OTransactionNoTx) { "Active transaction is detected. Changes in the schema must not happen in a transaction." }
}

internal fun requireNoActiveSession() {
    check(!hasActiveSession()) { "Active session is detected on the current thread" }
}

private fun hasActiveSession(): Boolean {
    val db = ODatabaseRecordThreadLocal.instance().getIfDefined()
    return db != null
}