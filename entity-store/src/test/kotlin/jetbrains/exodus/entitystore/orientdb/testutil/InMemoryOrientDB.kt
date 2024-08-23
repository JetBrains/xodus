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
package jetbrains.exodus.entitystore.orientdb.testutil

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.ODatabaseType
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import jetbrains.exodus.entitystore.orientdb.*
import jetbrains.exodus.entitystore.orientdb.testutil.Issues.Links.IN_PROJECT
import jetbrains.exodus.entitystore.orientdb.testutil.Issues.Links.ON_BOARD
import jetbrains.exodus.entitystore.orientdb.testutil.Projects.Links.HAS_ISSUE
import org.junit.rules.ExternalResource

class InMemoryOrientDB(
    private val initializeIssueSchema: Boolean = true,
    private val autoInitializeSchemaBuddy: Boolean = true
) : ExternalResource() {

    private lateinit var db: OrientDB
    lateinit var store: OPersistentEntityStore
        private set

    lateinit var provider: ODatabaseProviderImpl
    lateinit var schemaBuddy: OSchemaBuddyImpl

    val username = "admin"
    val password = "password"
    val dbName = "testDB"

    override fun before() {
        db = OrientDB("memory", OrientDBConfig.defaultConfig())
        db.execute("create database $dbName MEMORY users ( $username identified by '$password' role admin )")
        provider = ODatabaseProviderImpl(database, dbName, username, password, ODatabaseType.MEMORY)

        if (initializeIssueSchema) {
            provider.withSession { session ->
                session.getOrCreateVertexClass(Issues.CLASS)
                session.getOrCreateVertexClass(Boards.CLASS)
                session.getOrCreateVertexClass(Projects.CLASS)
                session.addAssociation(Issues.CLASS, Boards.CLASS, ON_BOARD, HAS_ISSUE)
                session.addAssociation(Boards.CLASS, Issues.CLASS, HAS_ISSUE, ON_BOARD)
                session.addAssociation(Issues.CLASS, Projects.CLASS, IN_PROJECT, HAS_ISSUE)
                session.addAssociation(Projects.CLASS, Issues.CLASS, HAS_ISSUE, IN_PROJECT)
            }
        }

        schemaBuddy = OSchemaBuddyImpl(provider, autoInitialize = autoInitializeSchemaBuddy)
        store = OPersistentEntityStore(provider, dbName,
            schemaBuddy = schemaBuddy
        )
    }

    override fun after() {
        db.close()
    }

    val database get() = db

    fun <R> withStoreTx(block: (OStoreTransaction) -> R): R {
        val tx = store.beginTransaction() as OStoreTransaction
        try {
            val result = block(tx)
            if (!tx.isFinished) {
                tx.commit()
            }
            return result
        } finally {
            if (!tx.isFinished) {
                tx.abort()
            }
        }
    }

    fun <R> withTxSession(block: (ODatabaseSession) -> R): R {
        val session = provider.acquireSession()
        try {
            session.begin()
            val result = block(session)
            if (session.hasActiveTransaction()) {
                session.commit()
            }
            return result
        } finally {
            if (!session.hasActiveTransaction()) {
                session.rollback()
            }
            if (!session.isClosed) {
                session.close()
            }
        }
    }

    fun <R> withSession(block: (ODatabaseSession) -> R): R {
        val session = provider.acquireSession()
        try {
            return block(session)
        } finally {
            if (!session.isClosed) {
                session.close()
            }
        }
    }

    fun openSession(): ODatabaseSession {
        return db.cachedPool(dbName, username, password).acquire()
    }
}
