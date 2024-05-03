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
import jetbrains.exodus.entitystore.orientdb.toEntityIterator
import mu.KLogging


class OQueryEntityIterator(private val source: Iterator<Entity>) : EntityIterator {

    companion object : KLogging() {

        val EMPTY = OQueryEntityIterator(emptyList<Entity>().iterator())

        fun executeAndCreate(query: OQuery, txn: OStoreTransaction): OQueryEntityIterator {
            val resultSet = query.execute(txn.activeSession)
            // Log execution plan
            val executionPlan = resultSet.executionPlan.get().prettyPrint(10, 8)
            val builder = StringBuilder()
            query.sql(builder)
            logger.info { "Query: $builder, params: ${query.params()}, \n execution plan:\n  $executionPlan, \n stats: ${resultSet.queryStats}" }

            val iterator = resultSet.toEntityIterator(txn.store as PersistentEntityStore)
            return OQueryEntityIterator(iterator)
        }
    }

    override fun next(): Entity {
        return source.next()
    }

    override fun hasNext(): Boolean {
        return source.hasNext()
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
