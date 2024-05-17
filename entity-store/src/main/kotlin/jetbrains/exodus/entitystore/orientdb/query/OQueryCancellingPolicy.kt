package jetbrains.exodus.entitystore.orientdb.query

import com.orientechnologies.common.concur.OTimeoutException
import jetbrains.exodus.entitystore.QueryCancellingPolicy

class OQueryTimeoutException(message: String, source: Throwable) : RuntimeException(message, source) {

    companion object {

        fun <R> withTimeoutWrap(block: () -> R): R {
            try {
                return block()
            } catch (e: OTimeoutException) {
                throw OQueryTimeoutException("Query execution timed out", e)
            }
        }
    }
}

interface OQueryCancellingPolicy : QueryCancellingPolicy {

    companion object {

        fun timeout(timeoutMillis: Long): OQueryCancellingPolicy = object : OQueryCancellingPolicy {
            override val timeoutMillis: Long = timeoutMillis
        }
    }

    /**
     * Not applicable for OStoreTransaction.
     */
    override fun needToCancel(): Boolean {
        return false
    }

    /**
     * Not applicable for OStoreTransaction.
     */
    override fun doCancel() {}

    /**
     * Defines the maximum time in milliseconds for the query.
     *
     * The field is read once priory to each query execution.
     */
    val timeoutMillis: Long
}