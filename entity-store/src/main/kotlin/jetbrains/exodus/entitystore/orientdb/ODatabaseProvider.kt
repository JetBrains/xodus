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
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.tx.OTransaction

interface ODatabaseProvider {
    val databaseLocation: String
    val database: OrientDB
    fun acquireSession(): ODatabaseSession
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

fun ODatabaseDocument.hasActiveTransaction(): Boolean {
    val tx = transaction
    return tx != null && tx.isActive
}

fun ODatabaseDocument.requireActiveTransaction(): OTransaction {
    require(hasActiveTransaction()) { "No active transaction is found. Happy debugging, pal!" }
    return transaction
}

fun ODatabaseDocument.requireNoActiveTransaction() {
    assert(transaction == null || !transaction.isActive) { "Active transaction is detected. Changes in the schema must not happen in a transaction." }
}

private fun hasActiveSession(): Boolean {
    val db = ODatabaseRecordThreadLocal.instance().getIfDefined()
    return db != null
}