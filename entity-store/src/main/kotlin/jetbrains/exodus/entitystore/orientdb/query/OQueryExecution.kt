package jetbrains.exodus.entitystore.orientdb.query

import com.orientechnologies.common.concur.OTimeoutException
import com.orientechnologies.orient.core.sql.executor.OResultSet
import jetbrains.exodus.entitystore.orientdb.OStoreTransaction

object OQueryExecution {

    fun execute(query: OQuery, tx: OStoreTransaction): OResultSet {
        val session = tx.activeSession
        val builder = StringBuilder()
        query.sql(builder)

        tx.queryCancellingPolicy?.let {
            check(it is OQueryCancellingPolicy) { "Unsupported query cancelling policy: $it" }
            val timeoutQuery = OQueryTimeout(it.timeoutMillis)
            timeoutQuery.sql(builder)
        }

        return try {
            session.query(builder.toString(), *query.params().toTypedArray())
        } catch (timeoutException: OTimeoutException) {
            throw OQueryTimeoutException("Query execution timed out", timeoutException)
        }
    }
}