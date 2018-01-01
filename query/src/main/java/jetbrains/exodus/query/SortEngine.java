/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
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
package jetbrains.exodus.query;


import jetbrains.exodus.entitystore.ComparableGetter;
import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.EntityIterable;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.entitystore.iterate.EntitiesOfTypeIterable;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.query.metadata.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@SuppressWarnings({"rawtypes", "NestedConditionalExpression"})
public class SortEngine {

    private static final int MAX_ENTRIES_TO_SORT_IN_MEMORY = Integer.getInteger("jetbrains.exodus.query.maxEntriesToSortInMemory", 100000);
    private static final int MAX_ENUM_COUNT_TO_SORT_LINKS = Integer.getInteger("jetbrains.exodus.query.maxEnumCountToSortLinks", 2048);
    private static final int MIN_ENTRIES_TO_SORT_LINKS = Integer.getInteger("jetbrains.exodus.query.minEntriesToSortLinks", 16);

    private static final Comparator<Comparable<Object>> PROPERTY_VALUE_COMPARATOR = new Comparator<Comparable<Object>>() {
        @Override
        public int compare(Comparable<Object> o1, Comparable<Object> o2) {
            return SortEngine.compareNullableComparables(o1, o2);
        }
    };
    private static final Comparator<Comparable<Object>> REVERSE_PROPERTY_VALUE_COMPARATOR = new Comparator<Comparable<Object>>() {
        @Override
        public int compare(Comparable<Object> o1, Comparable<Object> o2) {
            return SortEngine.compareNullableComparables(o2, o1);
        }
    };

    protected QueryEngine queryEngine;

    public SortEngine() {
    }

    public SortEngine(QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
    }

    public void setQueryEngine(QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
    }

    protected Entity attach(Entity entity) {
        return entity;
    }

    @Nullable
    protected Comparable getProperty(Entity entity, String propertyName) {
        return entity.getProperty(propertyName);
    }

    @Nullable
    protected Entity getLink(Entity entity, String linkName) {
        return entity.getLink(linkName);
    }

    @NotNull
    protected Iterable<Entity> getLinks(Entity entity, String linkName) {
        return entity.getLinks(linkName);
    }

    public Iterable<Entity> sort(String entityType, final String propertyName, Iterable<Entity> source, final boolean ascending) {
        ComparableGetter valueGetter = propertyGetter(propertyName);
        final ModelMetaData mmd = queryEngine.getModelMetaData();
        if (mmd != null) {
            final EntityMetaData emd = mmd.getEntityMetaData(entityType);
            if (emd != null) {
                if (source == null) {
                    return mergeSorted(emd, new IterableGetter() {
                        @Override
                        public EntityIterableBase getIterable(String type) {
                            queryEngine.assertOperational();
                            return (EntityIterableBase) queryEngine.getPersistentStore().getAndCheckCurrentTransaction().sort(type, propertyName, ascending);
                        }
                    }, valueGetter, caseInsensitiveComparator(ascending));
                }
                final Iterable<Entity> i = queryEngine.toEntityIterable(source);
                if (queryEngine.isPersistentIterable(i)) {
                    final EntityIterable it = ((EntityIterableBase) i).getSource();
                    if (it == EntityIterableBase.EMPTY) {
                        return queryEngine.wrap(EntityIterableBase.EMPTY);
                    }
                    if (it.getRoughCount() == 0 && it.count() == 0) {
                        return queryEngine.wrap(EntityIterableBase.EMPTY.asSortResult());
                    }
                    return mergeSorted(emd, new IterableGetter() {
                        @Override
                        public EntityIterableBase getIterable(String type) {
                            queryEngine.assertOperational();
                            return (EntityIterableBase) queryEngine.getPersistentStore().getAndCheckCurrentTransaction().sort(type, propertyName, it, ascending);
                        }
                    }, valueGetter, caseInsensitiveComparator(ascending));
                }
            }
        }
        if (source == null) {
            source = getAllEntities(entityType, mmd);
        }
        return sortInMemory(source, valueGetter, ascending);
    }

    @SuppressWarnings({"OverlyLongMethod", "OverlyNestedMethod"})
    public Iterable<Entity> sort(final String enumType, final String propName, final String entityType, final String linkName, Iterable<Entity> source, final boolean ascending) {
        ComparableGetter valueGetter = null;
        final ModelMetaData mmd = queryEngine.getModelMetaData();
        if (mmd != null) {
            final EntityMetaData emd = mmd.getEntityMetaData(entityType);
            if (emd != null) {
                final boolean isMultiple = emd.getAssociationEndMetaData(linkName).getCardinality().isMultiple();
                valueGetter = isMultiple ?
                        new ComparableGetter() {
                            @Override
                            public Comparable select(final Entity entity) {
                                // return the least property, to be replaced with getMin or something
                                Iterable<Entity> links = getLinks(entity, linkName);
                                Comparable result = null;
                                for (final Entity target : links) {
                                    final Comparable property = getProperty(target, propName);
                                    if (result == null) {
                                        result = property;
                                    } else {
                                        int compared = compareNullableComparables(result, property);
                                        if (ascending && compared > 0 || !ascending && compared < 0) {
                                            result = property;
                                        }
                                    }
                                }
                                return result;
                            }
                        } :
                        new ComparableGetter() {
                            @Override
                            public Comparable select(final Entity entity) {
                                final Entity target = getLink(entity, linkName);
                                return target == null ? null : getProperty(target, propName);
                            }
                        };
                final Iterable<Entity> i = queryEngine.toEntityIterable(source);
                if (queryEngine.isPersistentIterable(i)) {
                    final PersistentStoreTransaction txn = queryEngine.getPersistentStore().getAndCheckCurrentTransaction();
                    final EntityIterable s = ((EntityIterableBase) i).getSource();
                    if (s == EntityIterableBase.EMPTY) {
                        return queryEngine.wrap(EntityIterableBase.EMPTY);
                    }
                    final long sourceCount = s.getRoughCount();
                    if (sourceCount == 0 && s.count() == 0) {
                        return queryEngine.wrap(EntityIterableBase.EMPTY.asSortResult());
                    }
                    if (sourceCount < 0 || sourceCount >= MIN_ENTRIES_TO_SORT_LINKS) {
                        final EntityIterable it = ((EntityIterableBase) s).getOrCreateCachedInstance(txn);
                        EntityIterable allLinks = ((EntityIterableBase) queryEngine.queryGetAll(enumType).instantiate()).getSource();
                        final EntityIterable distinctLinks;
                        //TODO: maybe use EntityIterableBase and nonCachedHasFastCount
                        long enumCount = allLinks instanceof EntitiesOfTypeIterable ? allLinks.size() : allLinks.getRoughCount();
                        if (enumCount < 0 || enumCount > MAX_ENUM_COUNT_TO_SORT_LINKS) {
                            distinctLinks = ((EntityIterableBase) (isMultiple ?
                                    queryEngine.selectManyDistinct(it, linkName) :
                                    queryEngine.selectDistinct(it, linkName)
                            )).getSource();
                            enumCount = distinctLinks.getRoughCount();
                        } else {
                            distinctLinks = allLinks;
                        }
                        if (sourceCount > MAX_ENTRIES_TO_SORT_IN_MEMORY || enumCount <= MAX_ENUM_COUNT_TO_SORT_LINKS) {
                            final ComparableGetter linksGetter = propertyGetter(propName);
                            final EntityIterableBase distinctSortedLinks = mergeSorted(mmd.getEntityMetaData(enumType), new IterableGetter() {
                                @Override
                                public EntityIterableBase getIterable(String type) {
                                    queryEngine.assertOperational();
                                    return (EntityIterableBase) txn.sort(type, propName, distinctLinks, ascending);
                                }
                            }, linksGetter, caseInsensitiveComparator(ascending));
                            final AssociationEndMetaData aemd = emd.getAssociationEndMetaData(linkName);
                            if (aemd != null) {
                                AssociationMetaData amd = aemd.getAssociationMetaData();
                                if (amd.getType() != AssociationType.Directed) {
                                    final EntityMetaData oppositeEmd = aemd.getOppositeEntityMetaData();
                                    if (!(oppositeEmd.hasSubTypes())) {
                                        final String oppositeType = oppositeEmd.getType();
                                        final AssociationEndMetaData oppositeAemd = amd.getOppositeEnd(aemd);
                                        final String oppositeLinkName = oppositeAemd.getName();
                                        return mergeSorted(emd, new IterableGetter() {
                                            @Override
                                            public EntityIterableBase getIterable(String type) {
                                                queryEngine.assertOperational();
                                                return (EntityIterableBase) txn.sortLinks(type,
                                                        distinctSortedLinks.getSource(), isMultiple, linkName, it, oppositeType, oppositeLinkName);
                                            }
                                        }, valueGetter, caseInsensitiveComparator(ascending));
                                    }
                                }
                            }
                            return mergeSorted(emd, new IterableGetter() {
                                @Override
                                public EntityIterableBase getIterable(String type) {
                                    queryEngine.assertOperational();
                                    return (EntityIterableBase) txn.sortLinks(type, distinctSortedLinks.getSource(), isMultiple, linkName, it);
                                }
                            }, valueGetter, caseInsensitiveComparator(ascending));
                        } else {
                            // wrap source to avoid PersistentEntity instances to be exposed to transient level by in-memory sort (#JT-10189)
                            source = queryEngine.wrap(it);
                        }
                    } else {
                        // wrap source to avoid PersistentEntity instances to be exposed to transient level by in-memory sort (#JT-10189)
                        source = queryEngine.wrap(s);
                    }
                }
            }
        }
        if (source == null) {
            source = getAllEntities(entityType, mmd);
        }
        return sortInMemory(source, valueGetter, ascending);
    }

    protected Iterable<Entity> sort(Iterable<Entity> source, Comparator<Entity> comparator, boolean ascending) {
        return sortInMemory(source, ascending ? comparator : new ReverseComparator(comparator));
    }

    protected Iterable<Entity> sortInMemory(Iterable<Entity> source, Comparator<Entity> comparator) {
        if (source instanceof SortEngine.InMemorySortIterable) {
            final SortEngine.InMemorySortIterable merged = (SortEngine.InMemorySortIterable) source;
            return new InMemoryMergeSortIterable(source, new SortEngine.MergedComparator(merged.comparator, comparator));
        } else {
            return new InMemoryMergeSortIterable(source, comparator);
        }
    }

    protected Iterable<Entity> sortInMemory(Iterable<Entity> source, ComparableGetter valueGetter, boolean ascending) {
        if (source instanceof SortEngine.InMemorySortIterable) {
            final SortEngine.InMemorySortIterable merged = (SortEngine.InMemorySortIterable) source;
            final Comparator<Entity> comparator = new SortEngine.MergedComparator(merged.comparator, ascending
                    ? toComparator(valueGetter)
                    : new ReverseComparator(toComparator(valueGetter))
            );
            return new InMemoryMergeSortIterable(source, comparator);
        } else {
            return new InMemoryMergeSortIterableWithValueGetter(source, valueGetter, caseInsensitiveComparator(ascending));
        }
    }

    @NotNull
    private ComparableGetter propertyGetter(final String propertyName) {
        return new ComparableGetter() {
            @Override
            public Comparable select(Entity entity) {
                return getProperty(entity, propertyName);
            }
        };
    }

    private Iterable<Entity> getAllEntities(final String entityType, final ModelMetaData mmd) {
        queryEngine.assertOperational();
        EntityIterable it = queryEngine.instantiateGetAll(entityType);
        final EntityMetaData emd = mmd.getEntityMetaData(entityType);
        if (emd != null) {
            for (String subType : emd.getSubTypes()) {
                if (Utils.unionSubtypes()) {
                    it = ((EntityIterable) getAllEntities(subType, mmd)).union(it);
                } else {
                    it = ((EntityIterable) getAllEntities(subType, mmd)).concat(it);
                }
            }
        }
        return queryEngine.wrap(it);
    }

    private EntityIterableBase mergeSorted(EntityMetaData emd, IterableGetter sorted, final ComparableGetter valueGetter, final Comparator<Comparable<Object>> comparator) {
        EntityIterableBase result;
        if (!(emd.hasSubTypes())) {
            result = sorted.getIterable(emd.getType());
        } else {
            final List<EntityIterable> iterables = new ArrayList<>(4);
            EntityIterableBase source = sorted.getIterable(emd.getType()).getSource();
            if (source != EntityIterableBase.EMPTY) {
                iterables.add(source);
            }
            for (final String type : emd.getAllSubTypes()) {
                source = sorted.getIterable(type).getSource();
                if (source != EntityIterableBase.EMPTY) {
                    iterables.add(source);
                }
            }
            int iterablesCount = iterables.size();
            if (iterablesCount == 0) {
                result = EntityIterableBase.EMPTY;
            } else if (iterablesCount == 1) {
                result = (EntityIterableBase) iterables.get(0);
            } else {
                queryEngine.assertOperational();
                result = (EntityIterableBase) queryEngine.getPersistentStore().getAndCheckCurrentTransaction().mergeSorted(iterables, new ComparableGetter() {
                    @Override
                    public Comparable select(Entity entity) {
                        return valueGetter.select(attach(entity));
                    }
                }, comparator);
            }
        }
        return (EntityIterableBase) queryEngine.wrap(result.getSource().asSortResult());
    }

    public static int compareNullableComparables(Comparable c1, Comparable c2) {
        if (c1 == null && c2 == null) {
            return 0;
        }
        if (c1 == null) {
            return 1;
        }
        if (c2 == null) {
            return -1;
        }
        //noinspection unchecked
        return c1 instanceof String ? ((String) c1).compareToIgnoreCase((String) c2) : c1.compareTo(c2);
    }

    @NotNull
    private static Comparator<Comparable<Object>> caseInsensitiveComparator(boolean ascending) {
        return ascending ? PROPERTY_VALUE_COMPARATOR : REVERSE_PROPERTY_VALUE_COMPARATOR;
    }

    private static Comparator<Entity> toComparator(final ComparableGetter selector) {
        return new SortEngine.EntityComparator(selector);
    }

    private interface IterableGetter {
        EntityIterableBase getIterable(final String type);
    }

    @SuppressWarnings("ComparatorNotSerializable")
    private static class EntityComparator implements Comparator<Entity> {
        private final ComparableGetter selector;

        private EntityComparator(ComparableGetter selector) {
            this.selector = selector;
        }

        @Override
        public int compare(@NotNull final Entity o1, @NotNull final Entity o2) {
            Comparable c1 = selector.select(o1);
            Comparable c2 = selector.select(o2);
            return SortEngine.compareNullableComparables(c1, c2);
        }
    }

    @SuppressWarnings("ComparatorNotSerializable")
    private static class ReverseComparator implements Comparator<Entity> {
        private final Comparator<Entity> source;

        private ReverseComparator(Comparator<Entity> source) {
            this.source = source;
        }

        @Override
        public int compare(@NotNull final Entity o1, @NotNull final Entity o2) {
            return source.compare(o2, o1);
        }
    }

    @SuppressWarnings("ComparatorNotSerializable")
    private static class MergedComparator implements Comparator<Entity> {
        @NotNull
        private final Comparator<Entity> first;
        @NotNull
        private final Comparator<Entity> second;

        private MergedComparator(@NotNull final Comparator<Entity> first, @NotNull final Comparator<Entity> second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public int compare(@NotNull final Entity o1, @NotNull final Entity o2) {
            final int i = second.compare(o1, o2);
            if (i == 0) {
                return first.compare(o1, o2);
            }
            return i;
        }
    }

    public abstract static class InMemorySortIterable implements Iterable<Entity> {
        @NotNull
        protected final Iterable<Entity> source;
        @NotNull
        protected final Comparator<Entity> comparator;

        protected InMemorySortIterable(@NotNull final Iterable<Entity> source, @NotNull final Comparator<Entity> comparator) {
            this.source = source;
            this.comparator = comparator;
        }
    }
}
