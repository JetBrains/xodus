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
package jetbrains.exodus.entitystore.orientdb.iterate.merge

import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.entitystore.iterate.NonDisposableEntityIterator
import jetbrains.exodus.entitystore.orientdb.OQueryEntityIterable
import jetbrains.exodus.entitystore.orientdb.iterate.OQueryEntityIterableBase
import jetbrains.exodus.entitystore.orientdb.query.OSelect
import jetbrains.exodus.entitystore.orientdb.query.OUnionSelect
import java.util.*

class OMergeSortedEntityIterable(
    tx: StoreTransaction,
    private val sorted: List<EntityIterable>,
    private val valueGetter: (Entity) -> Comparable<Any?>,
    private val comparator: Comparator<Comparable<Any>?>
) : OQueryEntityIterableBase(tx) {

    override fun getIteratorImpl(txn: StoreTransaction): EntityIterator {
        return MergeSortedIterator(this)
    }

    override fun query(): OSelect {
        if (!sorted.all { it is OQueryEntityIterable }){
            throw UnsupportedOperationException("Not supported for non-OrientDB entity iterables")
        }
        if (sorted.size <= 1) {
            return (sorted[0] as OQueryEntityIterable).query()
        } else {
            val first = sorted[0] as OQueryEntityIterable
            val second = sorted[1] as OQueryEntityIterable
            return sorted.drop(2).fold(OUnionSelect(first.query(), second.query())) { acc, item ->
                item as OQueryEntityIterable
                OUnionSelect(acc, item.query())
            }
        }
    }

    inner class MergeSortedIterator(source: EntityIterableBase) : NonDisposableEntityIterator(source) {
        private val queue: PriorityQueue<EntityWithSource>

        init {

            queue = PriorityQueue<EntityWithSource>(
                this@OMergeSortedEntityIterable.sorted.size
            ) { o1: EntityWithSource, o2: EntityWithSource ->
                comparator.compare(
                    o1.value,
                    o2.value
                )
            }
            for (it in sorted) {
                val i = it.iterator()
                if (i.hasNext()) {
                    val id = i.nextId()
                    if (id != null) {
                        queue.add(EntityWithSource(id, i))
                    }
                }
            }
        }

        override fun hasNextImpl(): Boolean {
            return !queue.isEmpty()
        }

        public override fun nextIdImpl(): EntityId? {
            val pair = queue.poll()
            val result = pair.id
            val i = pair.source
            if (i.hasNext()) {
                queue.offer(EntityWithSource(i.nextId(), i))
            }
            return result
        }

        inner class EntityWithSource(val id: EntityId?, val source: EntityIterator) {
            var value: Comparable<Any>? = null
            init {
                if (id == null) {
                    this.value = null
                } else {
                    this.value = valueGetter(getEntity(id))
                }
            }
        }
    }
}
