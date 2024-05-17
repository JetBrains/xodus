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
package jetbrains.exodus.entitystore.orientdb.iterate

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityIterator
import jetbrains.exodus.entitystore.PersistentEntityStore
import jetbrains.exodus.entitystore.orientdb.OStoreTransaction
import jetbrains.exodus.entitystore.orientdb.query.OQuery
import jetbrains.exodus.entitystore.orientdb.query.OQueryExecution
import jetbrains.exodus.entitystore.orientdb.query.OQueryTimeoutException
import jetbrains.exodus.entitystore.orientdb.toEntityIterator


class OQueryEntityIterator(private val source: Iterator<Entity>) : EntityIterator {

    companion object {

        val EMPTY = OQueryEntityIterator(emptyList<Entity>().iterator())

        fun executeAndCreate(query: OQuery, txn: OStoreTransaction): OQueryEntityIterator {
            val resultSet = OQueryExecution.execute(query, txn)
            val iterator = resultSet.toEntityIterator(txn.store as PersistentEntityStore)
            return OQueryEntityIterator(iterator)
        }
    }

    override fun next(): Entity {
        return OQueryTimeoutException.withTimeoutWrap { source.next() }
    }

    override fun hasNext(): Boolean {
        return OQueryTimeoutException.withTimeoutWrap { source.hasNext() }
    }

    override fun skip(number: Int): Boolean {
        repeat(number) {
            if (!hasNext()) {
                return false
            }
            next()
        }
        return true
    }

    override fun nextId(): EntityId? {
        return next().id
    }

    override fun dispose(): Boolean {
        return true
    }

    override fun shouldBeDisposed(): Boolean {
        return false
    }

    override fun remove() {
        throw UnsupportedOperationException()
    }
}
