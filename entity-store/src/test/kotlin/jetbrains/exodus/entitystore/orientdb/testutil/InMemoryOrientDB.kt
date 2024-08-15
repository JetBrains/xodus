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
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.orientdb.*
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.BINARY_BLOB_CLASS_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.STRING_BLOB_CLASS_NAME
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
        val tx = store.beginTransaction()
        try {
            val result = block(ODatabaseSession.getActiveSession() as ODatabaseSession)
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

    fun <R> withSession(block: (ODatabaseSession) -> R): R {
        val session = openSession()
        try {
            return block(session)
        } finally {
            session.close()
        }
    }

    fun openSession(): ODatabaseSession {
        return db.cachedPool(dbName, username, password).acquire()
    }

    fun addAssociation(fromClass: OClass, toClass: OClass, outName: String, inName: String) {
        val linkInPropName = OVertex.getEdgeLinkFieldName(ODirection.IN, OVertexEntity.edgeClassName(inName))
        val linkOutPropName = OVertex.getEdgeLinkFieldName(ODirection.OUT, OVertexEntity.edgeClassName(outName))
        fromClass.createProperty(linkOutPropName, OType.LINKBAG, null as? OClass)
        toClass.createProperty(linkInPropName, OType.LINKBAG, null as? OClass)
    }
}
