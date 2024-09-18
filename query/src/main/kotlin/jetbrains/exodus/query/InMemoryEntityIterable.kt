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

import jetbrains.exodus.entitystore.*

class InMemoryEntityIterable(
    val iterable: Iterable<Entity>,
    private val txn: StoreTransaction,
    private val queryEngine: QueryEngine
) : EntityIterable {
    override fun iterator(): EntityIterator {
        return InMemoryEntityIterator(iterable.iterator())
    }

    override fun getTransaction(): StoreTransaction {
        return txn
    }

    override fun isEmpty(): Boolean {
        return !iterable.iterator().hasNext()
    }

    override fun size(): Long {
        return iterable.count().toLong()
    }

    override fun count(): Long {
        return iterable.count().toLong()
    }

    override fun getRoughCount(): Long {
        return iterable.count().toLong()
    }

    override fun getRoughSize(): Long {
        return iterable.count().toLong()
    }

    override fun indexOf(entity: Entity): Int {
        return iterable.indexOf(entity)
    }

    override fun contains(entity: Entity): Boolean {
        return iterable.contains(entity)
    }

    override fun intersect(right: EntityIterable): EntityIterable {
        return InMemoryEntityIterable(queryEngine.inMemoryIntersect(this, right), txn, queryEngine)
    }

    override fun intersectSavingOrder(right: EntityIterable): EntityIterable {
        //TODO this is may be wrong
        return InMemoryEntityIterable(queryEngine.inMemoryIntersect(this, right), txn, queryEngine)
    }

    override fun union(right: EntityIterable): EntityIterable {
        return InMemoryEntityIterable(queryEngine.inMemoryUnion(this, right), txn, queryEngine)
    }

    override fun minus(right: EntityIterable): EntityIterable {
        return InMemoryEntityIterable(queryEngine.inMemoryExclude(this, right), txn, queryEngine)
    }

    override fun concat(right: EntityIterable): EntityIterable {
        return InMemoryEntityIterable(queryEngine.inMemoryConcat(this, right), txn, queryEngine)
    }

    override fun skip(number: Int): EntityIterable {
        val skipIterator = Iterable {
            val i = iterator()
            i.skip(number)
            i
        }
        return InMemoryEntityIterable(skipIterator, txn, queryEngine)
    }

    override fun take(number: Int): EntityIterable {
        return InMemoryEntityIterable(iterable.take(number), txn, queryEngine)
    }

    override fun distinct(): EntityIterable {
        return InMemoryEntityIterable(iterable.distinct(), txn, queryEngine)
    }

    override fun selectDistinct(linkName: String): EntityIterable {
        val values = iterable.asSequence().mapNotNull {
            it.getLink(linkName)
        }.distinct()
        return InMemoryEntityIterable(values.asIterable(), txn, queryEngine)
    }

    override fun selectManyDistinct(linkName: String): EntityIterable {
        val values = iterable.asSequence().map { it.getLinks(linkName) }.flatten().distinct()
        return InMemoryEntityIterable(values.asIterable(), txn, queryEngine)
    }

    override fun getFirst(): Entity {
        return iterable.first()
    }

    override fun getLast(): Entity {
        return iterable.last()
    }

    override fun reverse(): EntityIterable {
        return InMemoryEntityIterable(iterable.reversed(), txn, queryEngine)
    }

    override fun isSortResult(): Boolean {
        return false
    }

    override fun asSortResult(): EntityIterable {
        throw NotImplementedError()
    }

    override fun unwrap(): EntityIterable {
        return this
    }

    override fun findLinks(entities: EntityIterable, linkName: String): EntityIterable {
        throw NotImplementedError()
    }
}

internal class InMemoryEntityIterator(val iterator: Iterator<Entity>) : EntityIterator {
    override fun remove() {
        throw UnsupportedOperationException()
    }

    override fun hasNext() = iterator.hasNext()

    override fun next() = iterator.next()

    override fun skip(number: Int): Boolean {
        for (i in 0..<number) {
            if (iterator.hasNext()) {
                iterator.next()
            } else {
                return false
            }
        }
        return true
    }

    override fun nextId(): EntityId {
        return next().id
    }

    override fun dispose() = true

    override fun shouldBeDisposed() = false

}
