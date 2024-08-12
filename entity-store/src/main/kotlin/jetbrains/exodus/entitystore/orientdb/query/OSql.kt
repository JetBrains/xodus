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

interface OSql {
    fun sql(builder: SqlBuilder)
}

class SqlBuilder {

    private val stringBuilder: StringBuilder = StringBuilder()
    private var varCounter = 0
    private var params: MutableMap<String, Any> = mutableMapOf()

    fun nextVarIndex(): Int {
        return varCounter++
    }

    fun append(value: Any): SqlBuilder {
        stringBuilder.append(value)
        return this
    }

    fun addParam(name: String, value: Any): String {
        val indexedName = "$name${nextVarIndex()}"
        params[indexedName] = value
        return indexedName
    }

    fun build(): SqlQuery {
        return SqlQuery(stringBuilder.toString(), params)
    }
}

data class SqlQuery(val sql: String, val params: Map<String, Any>)