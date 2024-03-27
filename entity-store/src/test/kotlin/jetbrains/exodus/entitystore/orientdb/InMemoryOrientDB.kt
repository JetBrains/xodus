package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import com.orientechnologies.orient.core.sql.executor.OResultSet
import org.junit.rules.ExternalResource

class InMemoryOrientDB(
    private val createClasses: Boolean = true
) : ExternalResource() {

    private lateinit var db: OrientDB
    lateinit var store:OPersistentStore
        private set

    val username = "admin"
    val password = "password"
    val dbName = "testDB"

    override fun before() {
        db = OrientDB("memory", OrientDBConfig.defaultConfig())
        db.execute("create database $dbName MEMORY users ( $username identified by '$password' role admin )")

        if (createClasses) {
            withSession { session ->
                session.createVertexClass(Issues.CLASS)
                session.createClass(OVertexEntity.STRING_BLOB_CLASS_NAME)
                session.createClass(OVertexEntity.BINARY_BLOB_CLASS_NAME)
            }
        }
        store = OPersistentStore(db, username, password, dbName)
    }

    override fun after() {
        db.close()
    }

    fun <R> withTxSession(block: (ODatabaseSession) -> R): R {
        val session = db.cachedPool(dbName, username, password).acquire()
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
        val session = db.cachedPool(dbName, username, password).acquire()
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
