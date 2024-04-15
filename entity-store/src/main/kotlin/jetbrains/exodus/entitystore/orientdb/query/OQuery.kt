package jetbrains.exodus.entitystore.orientdb.query

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.sql.executor.OResultSet

/**
 * Implementations must be immutable.
 */
interface OQuery {

    fun sql(): String
    fun params(): List<Any> = emptyList<Any>()

    fun execute(session: ODatabaseDocument? = null): OResultSet {
        ODatabaseSession.getActiveSession()
        val session = session ?: ODatabaseSession.getActiveSession()
        return session.query(sql(), *params().toTypedArray())
    }
}
