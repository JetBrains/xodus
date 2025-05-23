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
package jetbrains.exodus.entitystore.youtrackdb.iterate

import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityId
import jetbrains.exodus.entitystore.EntityIterator
import jetbrains.exodus.entitystore.youtrackdb.YTDBStoreTransaction
import jetbrains.exodus.entitystore.youtrackdb.query.YTDBQuery
import jetbrains.exodus.entitystore.youtrackdb.query.YTDBQueryExecution
import jetbrains.exodus.entitystore.youtrackdb.query.YTDBQueryTimeoutException
import jetbrains.exodus.entitystore.youtrackdb.toEntityIterator


class YTDBQueryEntityIterator(
    private val source: Iterator<Entity>,
    private var closed: Boolean = false,
    private val disposeResources: () -> Unit = {},
) : EntityIterator {

    companion object {

        val EMPTY = YTDBQueryEntityIterator(emptyList<Entity>().iterator(), closed = true)

        fun executeAndCreate(query: YTDBQuery, txn: YTDBStoreTransaction): YTDBQueryEntityIterator {
            val resultSet = YTDBQueryExecution.execute(query, txn)
            val iterator = resultSet.toEntityIterator(txn.getOEntityStore())
            return YTDBQueryEntityIterator(iterator) { resultSet.close() }
        }
    }

    override fun next(): Entity {
        return YTDBQueryTimeoutException.withTimeoutWrap { source.next() }
    }

    override fun hasNext(): Boolean {
        val hasNext = YTDBQueryTimeoutException.withTimeoutWrap { source.hasNext() }
        if (!hasNext) dispose()
        return hasNext
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

    override fun nextId(): EntityId {
        return next().id
    }

    override fun dispose(): Boolean {
        if (closed) {
            return false
        }
        closed = true
        disposeResources()
        return true
    }

    override fun shouldBeDisposed(): Boolean {
        return !closed
    }

    override fun remove() {
        throw UnsupportedOperationException()
    }
}
