package jetbrains.exodus.entitystore.orientdb.testutil

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.ODatabaseType
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import com.orientechnologies.orient.core.sql.executor.OResultSet
import jetbrains.exodus.entitystore.orientdb.ODatabaseProviderImpl
import jetbrains.exodus.entitystore.orientdb.OPersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.BINARY_BLOB_CLASS_NAME
import jetbrains.exodus.entitystore.orientdb.OVertexEntity.Companion.STRING_BLOB_CLASS_NAME
import jetbrains.exodus.entitystore.orientdb.getClassIdToOClassIdMap
import jetbrains.exodus.entitystore.orientdb.getOrCreateVertexClass
import jetbrains.exodus.entitystore.orientdb.testutil.Issues.CLASS
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

        if (createClasses) {
            withSession { session ->
                session.getOrCreateVertexClass(CLASS)
                session.createClass(STRING_BLOB_CLASS_NAME)
                session.createClass(BINARY_BLOB_CLASS_NAME)
            }
        }

        provider = ODatabaseProviderImpl(database, dbName, username, password, ODatabaseType.MEMORY)
        val classIdToOClassId = withSession { oSession ->
            oSession.getClassIdToOClassIdMap()
        }
        store = OPersistentEntityStore(provider, dbName, classIdToOClassId)
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
