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
package jetbrains.exodus.query

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.orientdb.OVertexEntity

open class SortEngine {

    lateinit var queryEngine: QueryEngine

    constructor()

    constructor(queryEngine: QueryEngine) {
        this.queryEngine = queryEngine
    }

    fun sort(entityType: String, propertyName: String, source: Iterable<Entity>, asc: Boolean): Iterable<Entity> {
        if (source is EntityIterable) {
            val txn = queryEngine.persistentStore.andCheckCurrentTransaction
            return txn.sort(entityType, propertyName, source.unwrap(), asc)
        } else {
            return if (asc) source.sortedBy { it.getProperty(propertyName) } else source.sortedByDescending { it.getProperty(propertyName) }
        }
    }

    fun sort(enumType: String, propName: String, entityType: String, linkName: String, source: Iterable<Entity>, asc: Boolean): Iterable<Entity> {
        if (source is EntityIterable) {
            val txn = queryEngine.persistentStore.andCheckCurrentTransaction
            return txn.sort(entityType, "$linkName.$propName", source.unwrap(), asc)
        } else {
            return if (asc) source.sortedBy { it.getLink(linkName)?.getProperty(propName) } else source.sortedByDescending { it.getLink(linkName)?.getProperty(propName) }
        }
    }

    protected fun sort(source: Iterable<Entity>, comparator: Comparator<Entity>, asc: Boolean): Iterable<Entity> {
        return if (asc) source.sortedWith(comparator) else source.sortedWith(comparator).reversed()
    }



}
