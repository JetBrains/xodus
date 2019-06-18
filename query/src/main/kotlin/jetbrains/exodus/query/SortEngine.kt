/**
 * Copyright 2010 - 2019 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.query

import jetbrains.exodus.ExodusException
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.iterate.EntitiesOfTypeIterable
import jetbrains.exodus.entitystore.iterate.EntityIterableBase
import jetbrains.exodus.query.metadata.AssociationType
import jetbrains.exodus.query.metadata.EntityMetaData
import jetbrains.exodus.query.metadata.ModelMetaData
import java.util.*

open class SortEngine {

    lateinit var queryEngine: QueryEngine

    constructor()

    constructor(queryEngine: QueryEngine) {
        this.queryEngine = queryEngine
    }

    fun sort(entityType: String, propertyName: String, source: Iterable<Entity>?, asc: Boolean): Iterable<Entity> {
        val txn = queryEngine.persistentStore.andCheckCurrentTransaction
        val valueGetter = propertyGetter(propertyName, txn.isReadonly)
        val mmd = queryEngine.modelMetaData
        if (mmd != null) {
            val emd = mmd.getEntityMetaData(entityType)
            if (emd != null) {
                if (source == null) {
                    return mergeSorted(emd, object : IterableGetter {
                        override fun getIterable(type: String): EntityIterableBase {
                            queryEngine.assertOperational()
                            return queryEngine.persistentStore.andCheckCurrentTransaction.sort(type, propertyName, asc) as EntityIterableBase
                        }
                    }, valueGetter, caseInsensitiveComparator(asc))
                }
                val i = queryEngine.toEntityIterable(source)
                if (queryEngine.isPersistentIterable(i)) {
                    val it = (i as EntityIterableBase).source
                    if (it === EntityIterableBase.EMPTY) {
                        return queryEngine.wrap(EntityIterableBase.EMPTY)
                    }
                    return if (it.roughCount == 0L && it.count() == 0L) {
                        queryEngine.wrap(EntityIterableBase.EMPTY.asSortResult())
                    } else mergeSorted(emd, object : IterableGetter {
                        override fun getIterable(type: String): EntityIterableBase {
                            queryEngine.assertOperational()
                            return queryEngine.persistentStore.andCheckCurrentTransaction.sort(type, propertyName, it, asc) as EntityIterableBase
                        }
                    }, valueGetter, caseInsensitiveComparator(asc))
                }
            }
        }
        return sortInMemory(source ?: getAllEntities(entityType, mmd), valueGetter, asc)
    }

    fun sort(enumType: String, propName: String, entityType: String, linkName: String, source: Iterable<Entity>?, asc: Boolean): Iterable<Entity> {
        var src = source
        val txn = queryEngine.persistentStore.andCheckCurrentTransaction
        var valueGetter: ComparableGetter? = null
        val mmd = queryEngine.modelMetaData
        if (mmd != null) {
            val emd = mmd.getEntityMetaData(entityType)
            if (emd != null) {
                val isReadOnlyTxn = txn.isReadonly
                val isMultiple = emd.getAssociationEndMetaData(linkName).cardinality.isMultiple
                valueGetter = if (isMultiple)
                    MultipleLinkComparableGetter(linkName, propName, asc, isReadOnlyTxn)
                else
                    SingleLinkComparableGetter(linkName, propName, txn)
                val i = queryEngine.toEntityIterable(src)
                if (queryEngine.isPersistentIterable(i) && i is EntityIterableBase) {
                    val s = i.source
                    if (s === EntityIterableBase.EMPTY) {
                        return queryEngine.wrap(EntityIterableBase.EMPTY)
                    }
                    val sourceCount = s.roughCount
                    if (sourceCount == 0L && s.count() == 0L) {
                        return queryEngine.wrap(EntityIterableBase.EMPTY.asSortResult())
                    }
                    if (sourceCount < 0 || sourceCount >= MIN_ENTRIES_TO_SORT_LINKS) {
                        val it = s.getOrCreateCachedInstance(txn)
                        val allLinks = (queryEngine.queryGetAll(enumType).instantiate() as EntityIterableBase).source
                        val distinctLinks: EntityIterable
                        var enumCount = (allLinks as? EntitiesOfTypeIterable)?.size() ?: allLinks.roughCount
                        if (enumCount < 0 || enumCount > MAX_ENUM_COUNT_TO_SORT_LINKS) {
                            distinctLinks = ((if (isMultiple)
                                queryEngine.selectManyDistinct(it, linkName)
                            else
                                queryEngine.selectDistinct(it, linkName)) as EntityIterableBase).source
                            enumCount = distinctLinks.getRoughCount()
                        } else {
                            distinctLinks = allLinks
                        }
                        if (sourceCount > MAX_ENTRIES_TO_SORT_IN_MEMORY || enumCount <= MAX_ENUM_COUNT_TO_SORT_LINKS) {
                            val linksGetter = propertyGetter(propName, isReadOnlyTxn)
                            val distinctSortedLinks = mergeSorted(mmd.getEntityMetaData(enumType)!!, object : IterableGetter {
                                override fun getIterable(type: String): EntityIterableBase {
                                    queryEngine.assertOperational()
                                    return txn.sort(type, propName, distinctLinks, asc) as EntityIterableBase
                                }
                            }, linksGetter, caseInsensitiveComparator(asc))
                            // check if all enums values are distinct
                            var enumsAreDistinct = true
                            if (i.isSortResult || s.isSortResult) {
                                var prev: Comparable<Any>? = null
                                distinctSortedLinks.filterNotNull().forEach { enum ->
                                    val current = getProperty(enum, propName, isReadOnlyTxn)
                                    if (current != null && prev?.compareTo(current) ?: 1 == 0) {
                                        enumsAreDistinct = false
                                        return@forEach
                                    }
                                    prev = current
                                }
                            }
                            if (enumsAreDistinct) {
                                val aemd = emd.getAssociationEndMetaData(linkName)
                                if (aemd != null) {
                                    val amd = aemd.associationMetaData
                                    if (amd.type != AssociationType.Directed) {
                                        val oppositeEmd = aemd.oppositeEntityMetaData
                                        if (!oppositeEmd.hasSubTypes()) {
                                            val oppositeType = oppositeEmd.type
                                            val oppositeAemd = amd.getOppositeEnd(aemd)
                                            val oppositeLinkName = oppositeAemd.name
                                            return mergeSorted(emd, object : IterableGetter {
                                                override fun getIterable(type: String): EntityIterableBase {
                                                    queryEngine.assertOperational()
                                                    return txn.sortLinks(type,
                                                            distinctSortedLinks.source, isMultiple, linkName, it, oppositeType, oppositeLinkName) as EntityIterableBase
                                                }
                                            }, valueGetter, caseInsensitiveComparator(asc))
                                        }
                                    }
                                }
                                return mergeSorted(emd, object : IterableGetter {
                                    override fun getIterable(type: String): EntityIterableBase {
                                        queryEngine.assertOperational()
                                        return txn.sortLinks(type, distinctSortedLinks.source, isMultiple, linkName, it) as EntityIterableBase
                                    }
                                }, valueGetter, caseInsensitiveComparator(asc))
                            } else {
                                src = queryEngine.wrap(it)
                            }
                        } else {
                            src = queryEngine.wrap(it)
                        }
                    } else {
                        src = queryEngine.wrap(s)
                    }
                }
            }
        }
        return sortInMemory(src ?: getAllEntities(entityType, mmd),
                valueGetter ?: throw ExodusException("ValueGetter is undefined"), asc)
    }

    protected open fun attach(entity: Entity): Entity {
        return entity
    }

    protected fun sort(source: Iterable<Entity>, comparator: Comparator<Entity>, asc: Boolean): Iterable<Entity> {
        return sortInMemory(source, if (asc) comparator else ReverseComparator(comparator))
    }

    protected fun sortInMemory(source: Iterable<Entity>, comparator: Comparator<Entity>): Iterable<Entity> {
        return if (source is SortEngine.InMemorySortIterable) {
            InMemoryMergeSortIterable(source, SortEngine.MergedComparator(source.comparator, comparator))
        } else {
            InMemoryMergeSortIterable(source, comparator)
        }
    }

    protected fun sortInMemory(source: Iterable<Entity>, valueGetter: ComparableGetter, asc: Boolean): Iterable<Entity> {
        return if (source is SortEngine.InMemorySortIterable) {
            val comparator = SortEngine.MergedComparator(source.comparator,
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

    private fun getProperty(entity: Entity, propertyName: String, readOnlyTxn: Boolean): Comparable<Any>? {
        return if (readOnlyTxn && entity !is PersistentEntity) {
            queryEngine.persistentStore.getEntity(entity.id).getProperty(propertyName)
        } else entity.getProperty(propertyName)
    }

    private fun getLinks(entity: Entity, linkName: String, readOnlyTxn: Boolean): Iterable<Entity> {
        return if (readOnlyTxn && entity !is PersistentEntity) {
            queryEngine.persistentStore.getEntity(entity.id).getLinks(linkName)
        } else entity.getLinks(linkName)
    }

    private fun propertyGetter(propertyName: String, readOnlyTxn: Boolean): ComparableGetter {
        return ComparableGetter { entity -> getProperty(entity, propertyName, readOnlyTxn) }
    }

    private fun getAllEntities(entityType: String, mmd: ModelMetaData?): Iterable<Entity> {
        queryEngine.assertOperational()
        val emd = mmd?.getEntityMetaData(entityType)
        var it = if (emd != null && emd.isAbstract)
            EntityIterableBase.EMPTY
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
        return queryEngine.wrap(it)
    }

    private fun mergeSorted(emd: EntityMetaData, sorted: IterableGetter, valueGetter: ComparableGetter, comparator: Comparator<Comparable<Any>>): EntityIterableBase {
        val result: EntityIterableBase
        if (!emd.hasSubTypes()) {
            result = sorted.getIterable(emd.type)
        } else {
            val iterables = ArrayList<EntityIterable>(4)
            var source = sorted.getIterable(emd.type).source
            if (source !== EntityIterableBase.EMPTY) {
                iterables.add(source)
            }
            for (type in emd.allSubTypes) {
                source = sorted.getIterable(type).source
                if (source !== EntityIterableBase.EMPTY) {
                    iterables.add(source)
                }
            }
            val iterablesCount = iterables.size
            result = when (iterablesCount) {
                0 -> EntityIterableBase.EMPTY
                1 -> iterables[0] as EntityIterableBase
                else -> {
                    queryEngine.assertOperational()
                    queryEngine.persistentStore.andCheckCurrentTransaction.mergeSorted(iterables,
                            ComparableGetter { entity -> valueGetter.select(attach(entity)) }, comparator) as EntityIterableBase
                }
            }
        }
        return queryEngine.wrap(result.source.asSortResult()) as EntityIterableBase
    }

    private interface IterableGetter {
        fun getIterable(type: String): EntityIterableBase
    }

    private class EntityComparator constructor(private val selector: ComparableGetter) : Comparator<Entity> {

        override fun compare(o1: Entity, o2: Entity): Int {
            val c1 = selector.select(o1)
            val c2 = selector.select(o2)
            return SortEngine.compareNullableComparables(c1, c2)
        }
    }

    private class ReverseComparator constructor(private val source: Comparator<Entity>) : Comparator<Entity> {

        override fun compare(o1: Entity, o2: Entity): Int {
            return source.compare(o2, o1)
        }
    }

    private class MergedComparator constructor(private val first: Comparator<Entity>, private val second: Comparator<Entity>) : Comparator<Entity> {

        override fun compare(o1: Entity, o2: Entity): Int {
            val i = second.compare(o1, o2)
            return if (i == 0) {
                first.compare(o1, o2)
            } else i
        }
    }

    abstract class InMemorySortIterable protected constructor(val src: Iterable<Entity>,
                                                              val comparator: Comparator<Entity>) : Iterable<Entity>

    private inner class MultipleLinkComparableGetter(private val linkName: String,
                                                     private val propName: String,
                                                     private val asc: Boolean,
                                                     private val readOnlyTxn: Boolean) : ComparableGetter {

        override fun select(entity: Entity): Comparable<Any>? {
            // return the least property, to be replaced with getMin or something
            val links = getLinks(entity, linkName, readOnlyTxn)
            var result: Comparable<Any>? = null
            for (target in links) {
                val property = getProperty(target, propName, readOnlyTxn)
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

    private inner class SingleLinkComparableGetter(private val linkName: String,
                                                   private val propName: String,
                                                   private val txn: PersistentStoreTransaction) : ComparableGetter {
        private val store = queryEngine.persistentStore
        private val linkId = store.getLinkId(txn, linkName, false)
        private val readOnlyTxn = txn.isReadonly

        override fun select(entity: Entity): Comparable<*>? {
            if (linkId < 0) return null
            val isPersistentEntity = entity is PersistentEntity
            val target: Entity?
            target = if (readOnlyTxn || isPersistentEntity) {
                val sourceId = entity.id
                val targetId = store.getRawLinkAsEntityId(txn, PersistentEntityId(sourceId), linkId)
                if (targetId == null) null else store.getEntity(targetId)
            } else {
                entity.getLink(linkName)
            }
            return if (target == null) null else getProperty(target, propName, readOnlyTxn)
        }
    }

    companion object {

        private val MAX_ENTRIES_TO_SORT_IN_MEMORY = Integer.getInteger("jetbrains.exodus.query.maxEntriesToSortInMemory", 10000000)
        private val MAX_ENUM_COUNT_TO_SORT_LINKS = Integer.getInteger("jetbrains.exodus.query.maxEnumCountToSortLinks", 2048)
        private val MIN_ENTRIES_TO_SORT_LINKS = Integer.getInteger("jetbrains.exodus.query.minEntriesToSortLinks", 16)

        private val PROPERTY_VALUE_COMPARATOR = Comparator<Comparable<Any>> { o1, o2 -> SortEngine.compareNullableComparables(o1, o2) }
        private val REVERSE_PROPERTY_VALUE_COMPARATOR = Comparator<Comparable<Any>> { o1, o2 -> SortEngine.compareNullableComparables(o2, o1) }

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
            return SortEngine.EntityComparator(selector)
        }
    }
}
