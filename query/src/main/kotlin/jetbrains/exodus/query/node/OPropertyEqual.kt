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
package jetbrains.exodus.query.node

import com.orientechnologies.orient.core.db.ODatabaseSession
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.orientdb.OIterableEntity
import jetbrains.exodus.query.NodeBase
import jetbrains.exodus.query.QueryEngine
import jetbrains.exodus.query.Utils.safe_equals
import jetbrains.exodus.query.metadata.ModelMetaData

class OPropertyEqual(
    private val name: String,
    private val value: Comparable<*>?
) : NodeBase() {

    fun getName(): String {
        return name
    }

    fun getValue(): Comparable<*>? {
        return value
    }

    override fun instantiate(entityType: String, queryEngine: QueryEngine, metaData: ModelMetaData, context: InstantiateContext): Iterable<Entity> {
        val session = ODatabaseSession.getActiveSession() ?: throw IllegalStateException("No active session")
        val query = "SELECT FROM $entityType WHERE $name = :$name"
        val params = mapOf(name to value)
        return OIterableEntity(session, query, params)
    }

    override fun getClone(): NodeBase {
        return OPropertyEqual(name, value)
    }

    override fun equals(obj: Any?): Boolean {
        if (obj === this) {
            return true
        }
        if (obj == null) {
            return false
        }
        checkWildcard(obj)
        if (obj !is OPropertyEqual) {
            return false
        }
        return safe_equals(name, obj.name) && safe_equals(value, obj.value)
    }

    override fun toString(prefix: String): String {
        return super.toString(prefix) + "($name=$value) "
    }

    override fun getHandle(sb: StringBuilder): StringBuilder? {
        return super.getHandle(sb).append('(').append(name).append('=').append(value).append(')')
    }

    override fun getSimpleName(): String {
        return "pe"
    }
}