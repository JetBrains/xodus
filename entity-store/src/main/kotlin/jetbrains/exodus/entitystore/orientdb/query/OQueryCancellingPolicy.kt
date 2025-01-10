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
package jetbrains.exodus.entitystore.orientdb.query

import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException
import jetbrains.exodus.entitystore.QueryCancellingPolicy

class OQueryTimeoutException(message: String, source: Throwable) : RuntimeException(message, source) {

    companion object {

        fun <R> withTimeoutWrap(block: () -> R): R {
            try {
                return block()
            } catch (e: TimeoutException) {
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