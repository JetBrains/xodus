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

import com.orientechnologies.orient.core.db.*
import jetbrains.exodus.entitystore.orientdb.ODatabaseProviderImpl
import jetbrains.exodus.entitystore.orientdb.OPersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OSchemaBuddyImpl
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.BINARY_BLOB_CLASS_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.STRING_BLOB_CLASS_NAME
import jetbrains.exodus.entitystore.orientdb.getOrCreateVertexClass
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
            provider.acquireSession().let { session->
                session.getOrCreateVertexClass(Issues.CLASS)
                session.getOrCreateVertexClass(Boards.CLASS)
                session.getOrCreateVertexClass(Projects.CLASS)
                session.createClass(STRING_BLOB_CLASS_NAME)
                session.createClass(BINARY_BLOB_CLASS_NAME)
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

    fun <R> withTxSession(block: (ODatabaseSession) -> R): R {
        val session = openSession()
        try {
            session.begin()
            val result = block(session)
            if (session.transaction.isActive) {
                session.commit()
            }
            return result
        } finally {
            session.close()
        }
    }

    fun <R> withSession(block: (ODatabaseSession) -> R): R {
        val txn = store.beginTransaction()
        val session = ODatabaseRecordThreadLocal.instance().get()
        try {
            return block(session)
        } finally {
            txn.commit()
            session.close()
        }
    }

    fun openSession(): ODatabaseSession {
        return db.cachedPool(dbName, username, password).acquire()
    }
}
