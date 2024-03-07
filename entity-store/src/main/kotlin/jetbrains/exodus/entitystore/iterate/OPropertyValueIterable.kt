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
package jetbrains.exodus.entitystore.iterate

import com.orientechnologies.orient.core.db.ODatabaseSession
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.orientdb.OEntityIterableHandle
import jetbrains.exodus.entitystore.orientdb.toEntityIterator

class OPropertyValueIterable(
    txn: PersistentStoreTransaction,
    private val entityType: String,
    private val propertyName: String,
    private val value: Comparable<*>
) : EntityIterableBase(txn) {

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIterator {
        val session = ODatabaseSession.getActiveSession() ?: throw IllegalStateException("No active session")
        val query = "SELECT FROM $entityType WHERE $propertyName = :$propertyName"
        val params = mapOf(propertyName to value)
        return session.query(query, params).toEntityIterator()
    }

    override fun getHandleImpl(): EntityIterableHandle {
        return OEntityIterableHandle("select from $entityType where $propertyName = :$propertyName")
    }
}
