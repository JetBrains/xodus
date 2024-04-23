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

import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.sql.executor.OResultSet

/**
 * Implementations must be immutable.
 */
interface OQuery : OSql {

    fun params(): List<Any> = emptyList<Any>()

    fun execute(session: ODatabaseDocument? = null): OResultSet {
        ODatabaseSession.getActiveSession()
        val session = session ?: ODatabaseSession.getActiveSession()
        return session.query(sql(), *params().toTypedArray())
    }
}
