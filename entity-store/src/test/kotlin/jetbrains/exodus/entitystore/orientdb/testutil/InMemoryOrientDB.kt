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
import com.orientechnologies.orient.core.sql.executor.OResultSet
import jetbrains.exodus.entitystore.orientdb.ODatabaseProviderImpl
import jetbrains.exodus.entitystore.orientdb.OPersistentEntityStore
import org.junit.rules.ExternalResource

class InMemoryOrientDB() : ExternalResource() {

    private lateinit var db: OrientDB
    lateinit var store: OPersistentEntityStore
        private set

    lateinit var provider: ODatabaseProviderImpl

    val username = "admin"
    val password = "password"
    val dbName = "testDB"

    override fun before() {
        db = OrientDB("memory", OrientDBConfig.defaultConfig())
        db.execute("create database $dbName MEMORY users ( $username identified by '$password' role admin )")
        provider = ODatabaseProviderImpl(database, dbName, username, password, ODatabaseType.MEMORY)
        store = OPersistentEntityStore(provider, dbName)
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
            session.commit()
            return result
        } finally {
            session.close()
        }
    }

    fun <R> withSession(block: (ODatabaseSession) -> R): R {
        val session = openSession()
        try {
            return block(session)
        } finally {
            session.close()
        }
    }

    fun withQuery(query: String, block: (OResultSet) -> Unit) {
        return withSession { session ->
            val result = session.query(query)
            block(result)
        }
    }

    fun openSession(): ODatabaseSession {
        return db.cachedPool(dbName, username, password).acquire()
    }
}
