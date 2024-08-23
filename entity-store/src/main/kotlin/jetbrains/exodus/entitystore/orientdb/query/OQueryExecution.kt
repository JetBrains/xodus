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

import com.orientechnologies.orient.core.sql.executor.OResultSet
import jetbrains.exodus.entitystore.orientdb.OStoreTransaction
import mu.KLogging

object OQueryExecution : KLogging() {

    fun execute(query: OQuery, tx: OStoreTransaction): OResultSet {
        val builder = SqlBuilder()
        query.sql(builder)
        tx.queryCancellingPolicy?.let {
            check(it is OQueryCancellingPolicy) { "Unsupported query cancelling policy: $it" }
            val timeoutQuery = OQueryTimeout(it.timeoutMillis)
            timeoutQuery.sql(builder)
        }

        val sqlQuery = builder.build()
        val resultSet = tx.query(sqlQuery.sql, sqlQuery.params)

        // Log execution plan
        // ToDo: add System param to enable/disable logging of execution plan
        logger.debug {
            val executionPlan = resultSet.executionPlan.get().prettyPrint(10, 8)
            "Query: $sqlQuery, \n execution plan:\n  $executionPlan, \n stats: ${resultSet.queryStats}"
        }

        return resultSet
    }
}