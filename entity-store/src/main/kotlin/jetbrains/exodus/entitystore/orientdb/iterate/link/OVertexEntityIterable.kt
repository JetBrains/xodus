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
package jetbrains.exodus.entitystore.orientdb.iterate.link

import com.orientechnologies.common.util.OSizeable
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.EntityIterator
import jetbrains.exodus.entitystore.PersistentEntityId
import jetbrains.exodus.entitystore.orientdb.OEntityId
import jetbrains.exodus.entitystore.orientdb.OEntityStore
import jetbrains.exodus.entitystore.orientdb.OStoreTransaction
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.util.unsupported
import org.apache.commons.collections4.IterableUtils
import org.apache.commons.collections4.IteratorUtils
import org.apache.commons.collections4.functors.EqualPredicate

class OVertexEntityIterable(
    private val tx: OStoreTransaction,
    private val vertices: Iterable<OVertex>,
    private val store: OEntityStore,
    private val linkName: String,
    private val targetEntityID: OEntityId
) : EntityIterable {

    override fun iterator() = object : EntityIterator {

        private val iterator = vertices.iterator()

        override fun skip(number: Int) =
            (0 until number).count { iterator.hasNext() }.also { iterator.next() } == number

        override fun nextId() = iterator.next().run { PersistentEntityId(identity.clusterId, identity.clusterPosition) }

        override fun dispose() = true

        override fun shouldBeDisposed() = false

        override fun hasNext() = iterator.hasNext()

        override fun next() = OVertexEntity(iterator.next(), store)

        override fun remove() = unsupported()
    }

    override fun getTransaction() = tx

    override fun isEmpty() = !vertices.iterator().hasNext()

    override fun size() = when (vertices) {
        is Collection<*> -> vertices.size.toLong()
        is OSizeable -> vertices.size().toLong()
        else -> IterableUtils.size(vertices).toLong()
    }

    override fun count() = when (vertices) {
        is Collection<*> -> vertices.size.toLong()
        is OSizeable -> vertices.size().toLong()
        else -> -1
    }

    override fun getRoughCount() = count()

    override fun getRoughSize() = count()

    override fun indexOf(entity: Entity) = IteratorUtils.indexOf(iterator(), EqualPredicate.equalPredicate(entity))

    override fun contains(entity: Entity) = IteratorUtils.contains(iterator(), entity)

    override fun intersect(right: EntityIterable) = asQueryIterable().intersect(right)

    override fun intersectSavingOrder(right: EntityIterable) = asQueryIterable().intersectSavingOrder(right)

    override fun union(right: EntityIterable) = asQueryIterable().union(right)

    override fun minus(right: EntityIterable) = asQueryIterable().minus(right)

    override fun concat(right: EntityIterable) = asQueryIterable().concat(right)

    //Here we may optimize it somehow, but have to store skip and so-on
    override fun skip(number: Int) = asQueryIterable().skip(number)

    override fun take(number: Int) = asQueryIterable().take(number)

    override fun distinct() = asQueryIterable().distinct()

    override fun selectDistinct(linkName: String) = asQueryIterable().selectDistinct(linkName)

    override fun selectManyDistinct(linkName: String) = asQueryIterable().selectManyDistinct(linkName)

    override fun getFirst() = iterator().run { if (hasNext()) next() else null }

    override fun getLast() = iterator().run { if (hasNext()) skip(count().toInt() - 1).run { next() } else null }

    override fun reverse() = asQueryIterable().reverse()

    override fun isSortResult() = false

    override fun asSortResult() = this

    override fun unwrap() = this

    private fun asQueryIterable() = OLinksFromEntityIterable(tx, linkName, this.targetEntityID)
}
