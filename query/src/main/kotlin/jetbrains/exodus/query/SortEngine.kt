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

import jetbrains.exodus.entitystore.ComparableGetter
import jetbrains.exodus.entitystore.Entity
import jetbrains.exodus.entitystore.EntityIterable
import jetbrains.exodus.entitystore.orientdb.OEntityIterable
import jetbrains.exodus.entitystore.orientdb.OVertexEntity
import jetbrains.exodus.entitystore.orientdb.iterate.OEntityIterableBase
import jetbrains.exodus.query.metadata.ModelMetaData

open class SortEngine {

    lateinit var queryEngine: QueryEngine

    constructor()

    constructor(queryEngine: QueryEngine) {
        this.queryEngine = queryEngine
    }

    fun sort(entityType: String, propertyName: String, source: Iterable<Entity>?, asc: Boolean): Iterable<Entity> {
        val txn = queryEngine.persistentStore.andCheckCurrentTransaction
        val valueGetter = propertyGetter(propertyName)
        val mmd = queryEngine.modelMetaData
        if (mmd != null) {
            val emd = mmd.getEntityMetaData(entityType)
            if (emd != null) {
                if (source == null) {
                    return txn.sort(entityType, propertyName, asc)
                }
                val i = queryEngine.toEntityIterable(source)
                if (queryEngine.isPersistentIterable(i)) {
                    val it = (i as OEntityIterableBase).unwrap()
                    if (it === OEntityIterableBase.EMPTY) {
                        OEntityIterableBase.EMPTY
                    }
                    return if (it.roughCount == 0L && it.count() == 0L) {
                        OEntityIterableBase.EMPTY
                    } else {
                        txn.sort(entityType, propertyName, (source as EntityIterable).unwrap(), asc)
                    }
                }
            }
        }
        return sortInMemory(source ?: getAllEntities(entityType, mmd), valueGetter, asc)
    }

    fun sort(
        enumType: String,
        propName: String,
        entityType: String,
        linkName: String,
        source: Iterable<Entity>,
        asc: Boolean
    ): Iterable<Entity> {
        if (source is OEntityIterable) {
            val txn = queryEngine.persistentStore.andCheckCurrentTransaction
            return txn.sort(entityType, "${OVertexEntity.edgeClassName(linkName)}.$propName", source.unwrap(), asc)
        } else {
            val mmd = queryEngine.modelMetaData!!
            val emd = mmd.getEntityMetaData(entityType)!!
            val isMultiple = emd.getAssociationEndMetaData(linkName).cardinality.isMultiple
            val valueGetter = if (isMultiple)
                MultipleLinkComparableGetter(linkName, propName, asc)
            else
                SingleLinkComparableGetter(linkName, propName)
            return sortInMemory(
                source,
                valueGetter, asc
            )
        }
    }

    protected fun sort(source: Iterable<Entity>, comparator: Comparator<Entity>, asc: Boolean): Iterable<Entity> {
        return sortInMemory(source, if (asc) comparator else ReverseComparator(comparator))
    }

    private fun sortInMemory(source: Iterable<Entity>, comparator: Comparator<Entity>): Iterable<Entity> {
        return if (source is InMemorySortIterable) {
            InMemoryMergeSortIterable(source, MergedComparator(source.comparator, comparator))
        } else {
            InMemoryMergeSortIterable(source, comparator)
        }
    }

    private fun sortInMemory(source: Iterable<Entity>, valueGetter: ComparableGetter, asc: Boolean): Iterable<Entity> {
        return if (source is InMemorySortIterable) {
            val comparator = MergedComparator(
                source.comparator,
                if (asc)
                    toComparator(valueGetter)
                else
                    ReverseComparator(toComparator(valueGetter))
            )
            InMemoryMergeSortIterable(source, comparator)
        } else {
            InMemoryMergeSortIterableWithValueGetter(source, valueGetter, caseInsensitiveComparator(asc))
        }
    }

    private fun propertyGetter(propertyName: String): ComparableGetter {
        return ComparableGetter { entity -> entity.getProperty(propertyName) }
    }

    private fun getAllEntities(entityType: String, mmd: ModelMetaData?): Iterable<Entity> {
        queryEngine.assertOperational()
        val emd = mmd?.getEntityMetaData(entityType)
        var it = if (emd != null && emd.isAbstract)
            OEntityIterableBase.EMPTY
        else
            queryEngine.instantiateGetAll(entityType)
        if (emd != null) {
            for (subType in emd.subTypes) {
                it = if (Utils.unionSubtypes) {
                    (getAllEntities(subType, mmd) as EntityIterable).union(it)
                } else {
                    (getAllEntities(subType, mmd) as EntityIterable).concat(it)
                }
            }
        }
        return it
    }

    private interface IterableGetter {
        fun getIterable(type: String): OEntityIterableBase
    }

    private class EntityComparator(private val selector: ComparableGetter) : Comparator<Entity> {

        override fun compare(o1: Entity, o2: Entity): Int {
            val c1 = selector.select(o1)
            val c2 = selector.select(o2)
            return compareNullableComparables(c1, c2)
        }
    }

    private class ReverseComparator(private val source: Comparator<Entity>) : Comparator<Entity> {

        override fun compare(o1: Entity, o2: Entity): Int {
            return source.compare(o2, o1)
        }
    }

    private class MergedComparator(private val first: Comparator<Entity>, private val second: Comparator<Entity>) :
        Comparator<Entity> {

        override fun compare(o1: Entity, o2: Entity): Int {
            val i = second.compare(o1, o2)
            return if (i == 0) {
                first.compare(o1, o2)
            } else i
        }
    }

    abstract class InMemorySortIterable protected constructor(
        val src: Iterable<Entity>,
        val comparator: Comparator<Entity>
    ) : Iterable<Entity>

    private inner class MultipleLinkComparableGetter(
        private val linkName: String,
        private val propName: String,
        private val asc: Boolean
    ) : ComparableGetter {

        override fun select(entity: Entity): Comparable<Any>? {
            // return the least property, to be replaced with getMin or something
            val links = entity.getLinks(linkName)
            var result: Comparable<Any>? = null
            for (target in links) {
                val property = target.getProperty(propName)
                if (result == null) {
                    result = property
                } else {
                    val compared = compareNullableComparables(result, property)
                    if (asc && compared > 0 || !asc && compared < 0) {
                        result = property
                    }
                }
            }
            return result
        }
    }

    private inner class SingleLinkComparableGetter(
        private val linkName: String,
        private val propName: String,
    ) : ComparableGetter {

        override fun select(entity: Entity): Comparable<*>? {
            return entity.getLink(linkName)?.getProperty(propName)
        }
    }

    companion object {

        private val PROPERTY_VALUE_COMPARATOR =
            Comparator<Comparable<Any>> { o1, o2 -> compareNullableComparables(o1, o2) }
        private val REVERSE_PROPERTY_VALUE_COMPARATOR =
            Comparator<Comparable<Any>> { o1, o2 -> compareNullableComparables(o2, o1) }

        fun compareNullableComparables(c1: Comparable<Any>?, c2: Comparable<Any>?): Int {
            if (c1 == null && c2 == null) {
                return 0
            }
            if (c1 == null) {
                return 1
            }
            return if (c2 == null) {
                -1
            } else (c1 as? String)?.compareTo((c2 as String), ignoreCase = true) ?: c1.compareTo(c2)

        }

        private fun caseInsensitiveComparator(asc: Boolean): Comparator<Comparable<Any>> {
            return if (asc) PROPERTY_VALUE_COMPARATOR else REVERSE_PROPERTY_VALUE_COMPARATOR
        }

        private fun toComparator(selector: ComparableGetter): Comparator<Entity> {
            return EntityComparator(selector)
        }
    }
}
