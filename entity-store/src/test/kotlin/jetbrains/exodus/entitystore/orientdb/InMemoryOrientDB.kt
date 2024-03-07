package jetbrains.exodus.entitystore.orientdb

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import com.orientechnologies.orient.core.sql.executor.OResultSet
import org.junit.rules.ExternalResource

class InMemoryOrientDB : ExternalResource() {

    private lateinit var db: OrientDB

    val username = "admin"
    val password = "admin"
    val dbName = "testDB"

    override fun before() {
        db = OrientDB("memory", OrientDBConfig.defaultConfig())
        db.execute("create database $dbName MEMORY users ( $username identified by '$password' role admin )")

        withSessionNoTx { session ->
            session.createVertexClass(IssueClass.NAME)
            session.createClass(OVertexEntity.STRING_BLOB_CLASS_NAME)
            session.createClass(OVertexEntity.BINARY_BLOB_CLASS_NAME)
        }
    }

    override fun after() {
        db.close()
    }

    fun <R> withSession(block: (ODatabaseSession) -> R): R {
        val session = db.open(dbName, username, password)
        try {
            session.begin()
            val result = block(session)
            session.commit()
            return result
        } finally {
            session.close()
        }
    }

    fun <R> withSessionNoTx(block: (ODatabaseSession) -> R): R {
        val session = db.open(dbName, username, password)
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
        return db.open(dbName, username, password)
    }
}
