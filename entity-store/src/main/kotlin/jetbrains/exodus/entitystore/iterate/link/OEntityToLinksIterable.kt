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
package jetbrains.exodus.entitystore.iterate.link

import com.orientechnologies.common.util.OSizeable
import com.orientechnologies.orient.core.record.OVertex
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.EntityIterator
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.util.unsupported
import org.apache.commons.collections4.IterableUtils
import org.apache.commons.collections4.IteratorUtils
import org.apache.commons.collections4.functors.EqualPredicate

class OEntityToLinksIterable(private val vertices: Iterable<OVertex>) : EntityIterable {

    override fun iterator() = object : EntityIterator {

        private val iterator = vertices.iterator()

        override fun skip(number: Int) =
            (0 until number).count { iterator.hasNext() }.also { iterator.next() } == number

        override fun nextId() = iterator.next().run { PersistentEntityId(identity.clusterId, identity.clusterPosition) }

        override fun dispose() = true

        override fun shouldBeDisposed() = false

        override fun hasNext() = iterator.hasNext()

        override fun next() = OVertexEntity(iterator.next())

        override fun remove() = unsupported()
    }

    override fun getTransaction() = unsupported()

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

    override fun intersect(right: EntityIterable) = unsupported()

    override fun intersectSavingOrder(right: EntityIterable) = unsupported()

    override fun union(right: EntityIterable) = unsupported()

    override fun minus(right: EntityIterable) = unsupported()

    override fun concat(right: EntityIterable) = unsupported()

    override fun skip(number: Int) = unsupported()

    override fun take(number: Int) = unsupported()

    override fun distinct() = unsupported()

    override fun selectDistinct(linkName: String) = unsupported()

    override fun selectManyDistinct(linkName: String) = unsupported()

    override fun getFirst() = iterator().run { if (hasNext()) next() else null }

    override fun getLast() = iterator().run { if (hasNext()) skip(count().toInt() - 1).run { next() } else null }

    override fun reverse() = unsupported()

    override fun isSortResult() = false

    override fun asSortResult() = this
}