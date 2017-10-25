/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.core.dataStructures.hash.LongHashMap;
import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import jetbrains.exodus.core.dataStructures.hash.LongSet;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.tables.PropertyValue;
import jetbrains.exodus.util.MathUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

@SuppressWarnings({"unchecked", "rawtypes"})
public final class SortIterable extends EntityIterableDecoratorBase {

    @NotNull
    private final EntityIterableBase propIndex;
    private final int sourceTypeId;
    private final int propertyId;
    private final boolean ascending;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new SortIterable(txn, (EntityIterableBase) parameters[3],
                    (EntityIterableBase) parameters[3], Integer.valueOf((String) parameters[0]),
                    Integer.valueOf((String) parameters[1]), "0".equals(parameters[2]));
            }
        });
    }

    public SortIterable(@NotNull final PersistentStoreTransaction txn,
                        @NotNull final EntityIterableBase propIndex,
                        @NotNull final EntityIterableBase source,
                        final int sourceTypeId,
                        final boolean ascending) {
        this(txn, propIndex, source, sourceTypeId, -1, ascending);
    }

    public SortIterable(@NotNull final PersistentStoreTransaction txn,
                        @NotNull final EntityIterableBase propIndex,
                        @NotNull final EntityIterableBase source,
                        final int sourceTypeId,
                        final int propertyId,
                        final boolean ascending) {
        super(txn, source);
        this.propIndex = propIndex;
        this.sourceTypeId = sourceTypeId;
        this.propertyId = propertyId;
        this.ascending = ascending;
    }

    @Override
    public int getEntityTypeId() {
        return sourceTypeId;
    }

    @Override
    public boolean setOrigin(Object origin) {
        if (super.setOrigin(origin)) {
            propIndex.setOrigin(origin);
            return true;
        }
        return false;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.SORTING;
    }

    @Override
    public boolean isEmpty() {
        return source.isEmpty();
    }

    @Override
    public long size() {
        return source.size();
    }

    @Override
    public long count() {
        return source.count();
    }

    @Override
    public long getRoughCount() {
        return source.getRoughCount();
    }

    @Override
    public long getRoughSize() {
        return source.getRoughSize();
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        int count = 0;
        final EntityIterator sorted = new EntityTypeFilteredIterator(source, sourceTypeId);
        while (sorted.hasNext()) {
            sorted.nextId();
            ++count;
        }
        return count;
    }

    @Override
    @NotNull
    public EntityIterator getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        if (propIndex == EntityIterableBase.EMPTY) {
            return new EntityTypeFilteredIterator(source, sourceTypeId);
        }
        final PersistentEntityStoreImpl store = getStore();
        final EntityIterableBase cachedPropertyIndex = store.getEntityIterableCache().putIfNotCached(propIndex);

        if (propIndex.nonCachedHasFastCountAndIsEmpty() && store.getConfig().isDebugAllowInMemorySort()) {
            // if property index is much greater than source then it makes sense to sort source in-memory (XD-609)
            final long sourceSize = source.getRoughCount();
            if (sourceSize >= 0) {
                final long indexSize = cachedPropertyIndex.size();
                final long log2IndexSize = MathUtil.longLogarithm(indexSize);
                final long sizeMulLog = sourceSize * log2IndexSize;
                final boolean isCachedInstance = cachedPropertyIndex.isCachedInstance();
                if ((isCachedInstance && sizeMulLog * sourceSize < indexSize) ||
                    (!isCachedInstance && sizeMulLog * log2IndexSize < indexSize)) {
                    return new StableInMemorySortIterator((int) sourceSize);
                }
            }
        }

        final EntityIterator propIterator = ascending ?
            cachedPropertyIndex.getIteratorImpl(txn) : cachedPropertyIndex.getReverseIteratorImpl(txn);
        if (propIterator.shouldBeDisposed()) {
            store.getAndCheckCurrentTransaction().registerEntityIterator(propIterator);
        }
        if (propIterator == EntityIteratorBase.EMPTY) {
            return new EntityTypeFilteredIterator(source, sourceTypeId);
        }
        if (source.isSortResult()) {
            final StableSortIterator itr = new StableSortIterator((PropertyValueIterator) propIterator);
            return new PropertyValueIteratorFixingDecorator(this, itr, itr);
        }
        return new EntityIteratorFixingDecorator(this, new NonStableSortIterator(txn, propIterator));
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new EntityIterableHandleDecorator(getStore(), SortIterable.getType(), source.getHandle()) {
            @NotNull
            private final int[] propertyIds = mergeFieldIds(new int[]{propertyId}, decorated.getPropertyIds());

            @NotNull
            @Override
            public int[] getPropertyIds() {
                return propertyIds;
            }

            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                builder.append(sourceTypeId);
                builder.append('-');
                builder.append(propertyId);
                builder.append('-');
                applyDecoratedToBuilder(builder);
                builder.append('-');
                builder.append(ascending ? 0 : 1);
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                hash.apply(sourceTypeId);
                hash.applyDelimiter();
                hash.apply(propertyId);
                hash.applyDelimiter();
                super.hashCode(hash);
                hash.applyDelimiter();
                hash.apply(ascending ? 0 : 1);
            }

            @Override
            public int getEntityTypeId() {
                return sourceTypeId;
            }

            @Override
            public boolean isMatchedPropertyChanged(final int typeId,
                                                    final int propertyId,
                                                    @Nullable final Comparable oldValue,
                                                    @Nullable final Comparable newValue) {
                return sourceTypeId == typeId &&
                    (decorated.isMatchedPropertyChanged(typeId, propertyId, oldValue, newValue) ||
                        propIndex.getHandle().isMatchedPropertyChanged(typeId, propertyId, oldValue, newValue));
            }
        };
    }

    @Override
    public boolean canBeCached() {
        return source.isThreadSafe();
    }

    @SuppressWarnings({"MethodOnlyUsedFromInnerClass"})
    @NotNull
    private LongHashMap<Integer> getRightOrder() {
        final LongHashMap<Integer> result = new LongHashMap<>();
        int position = 0;
        final EntityIterator sorted = source.iterator();
        while (sorted.hasNext()) {
            final EntityId entityId = sorted.nextId();
            if (entityId == null) {
                result.put(Long.MAX_VALUE, (Integer) position++);
            } else if (sourceTypeId == entityId.getTypeId()) {
                result.put(entityId.getLocalId(), (Integer) position++);
            }
        }
        return result;
    }

    private final class StableSortIterator extends NonDisposableEntityIterator implements PropertyValueIterator {

        @NotNull
        private final PropertyValueIterator propertyValueIterator;
        private final PriorityQueue<Long> sameValueQueue;
        private final LongHashMap<Integer> rightOrder;
        private long nextId;
        private long lastEntityId;
        private Comparable currentValue;
        private Comparable lastValue;

        private StableSortIterator(@NotNull final PropertyValueIterator propertyValueIterator) {
            super(propIndex);
            this.propertyValueIterator = propertyValueIterator;
            sameValueQueue = new PriorityQueue<>(4, new Comparator<Long>() {
                @Override
                public int compare(final Long o1, final Long o2) {
                    return rightOrder.get(o1) - rightOrder.get(o2);
                }
            });
            rightOrder = getRightOrder();
            currentValue = null;
            lastValue = null;
        }

        @SuppressWarnings({"OverlyLongMethod"})
        @Override
        protected boolean hasNextImpl() {
            if (sameValueQueue.isEmpty()) {
                Comparable lastValue = this.lastValue;
                if (lastValue != null) {
                    currentValue = lastValue;
                    sameValueQueue.offer(lastEntityId);
                    this.lastValue = null;
                }
                while (propertyValueIterator.hasNext()) {
                    // property-value index can't return null since there cannot be an entity with null id
                    //noinspection ConstantConditions
                    final long nextId = propertyValueIterator.nextId().getLocalId();
                    if (rightOrder.containsKey(nextId)) {
                        final Comparable currentValue = propertyValueIterator.currentValue();
                        if (currentValue != null && lastValue != null && lastValue.compareTo(currentValue) != 0) {
                            this.lastValue = currentValue;
                            lastEntityId = nextId;
                            break;
                        }
                        lastValue = currentValue;
                        this.currentValue = currentValue;
                        sameValueQueue.offer(nextId);
                    }
                }
            }
            while (true) {
                if (!sameValueQueue.isEmpty()) {
                    final long id = sameValueQueue.poll();
                    nextId = id;
                    rightOrder.remove(id);
                    return true;
                }
                // there is no element in the sameValueQueue, so there is no current value
                // right order still can contain something (including null)
                currentValue = null;
                if (rightOrder.isEmpty()) {
                    break;
                }
                for (long localId : rightOrder.keySet()) {
                    sameValueQueue.offer(localId);
                }
            }
            return false;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            final long nextId = this.nextId;
            return nextId == Long.MAX_VALUE ? null : new PersistentEntityId(sourceTypeId, nextId);
        }

        @Override
        public Comparable currentValue() {
            return currentValue;
        }
    }

    private final class NonStableSortIterator extends NonDisposableEntityIterator {

        @NotNull
        private final EntityIterator propIterator;
        private EntityId nextId;
        private final LongSet rightOrder;
        private LongIterator rightOrderIt;
        private boolean hasNull;

        private NonStableSortIterator(@NotNull final PersistentStoreTransaction txn,
                                      @NotNull final EntityIterator propIterator) {
            super(propIndex);
            this.propIterator = propIterator;
            final EntityIdSet sourceSet = source.toSet(txn);
            hasNull = sourceSet.contains(null);
            rightOrder = sourceSet.getTypeSetSnapshot(sourceTypeId);
            nextId = null;
        }

        @Override
        protected boolean hasNextImpl() {
            while (!rightOrder.isEmpty() && propIterator.hasNext()) {
                final EntityId nextId = propIterator.nextId();
                if (nextId != null && rightOrder.remove(nextId.getLocalId())) {
                    this.nextId = nextId;
                    return true;
                }
            }
            if (rightOrderIt == null) {
                rightOrderIt = rightOrder.iterator();
            }
            final LongIterator it = rightOrderIt;
            if (it.hasNext()) {
                final long localId = it.nextLong();
                nextId = new PersistentEntityId(sourceTypeId, localId);
                return true;
            }
            if (hasNull) {
                nextId = null;
                hasNull = false;
                return true;
            }
            return false;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            return nextId;
        }
    }

    // TODO:
    //    1. Get rid of using property name for getting property value
    //    2. Consider using Keap for lazy sorting
    private final class StableInMemorySortIterator extends NonDisposableEntityIterator implements PropertyValueIterator {

        private final List<IdValuePair> pairs;
        private boolean hasNull;
        private int cursor;
        private Comparable currentValue;

        private StableInMemorySortIterator(final int sourceSize) {
            super(propIndex);

            pairs = new ArrayList<>(sourceSize / 2);
            hasNull = false;
            cursor = 0;

            final PersistentEntityStoreImpl store = getStore();
            final PersistentStoreTransaction txn = getTransaction();
            final String propertyName = store.getPropertyName(txn, propertyId);
            if (propertyName == null) {
                throw new NullPointerException("Property name is null");
            }

            final int propertyId = store.getPropertyId(txn, propertyName, false);
            if (propertyId < 0) {
                throw new IllegalStateException("Property name is not registered");
            }

            final EntityIterator it = source.iterator();

            while (it.hasNext()) {
                final PersistentEntityId nextId = (PersistentEntityId) it.nextId();
                if (nextId == null) {
                    hasNull = true;
                } else if (nextId.getTypeId() == sourceTypeId) {
                    final PropertyValue propValue = store.getPropertyValue(txn, new PersistentEntity(store, nextId), propertyId);
                    pairs.add(new IdValuePair(nextId.getLocalId(), propValue == null ? null : propValue.getData()));
                }
            }

            // finally sort
            final Object[] array = pairs.toArray();
            Arrays.sort(array, new Comparator() {
                @Override
                public int compare(final Object o1, final Object o2) {
                    final Comparable propValue1 = ((IdValuePair) o1).propValue;
                    final Comparable propValue2 = ((IdValuePair) o2).propValue;
                    if (propValue1 == null && propValue2 == null) {
                        return 0;
                    }
                    if (propValue1 == null) {
                        return -1;
                    }
                    if (propValue2 == null) {
                        return 1;
                    }
                    return propValue1.compareTo(propValue2);
                }
            });
            final ListIterator<IdValuePair> i = pairs.listIterator();
            for (final Object o : array) {
                i.next();
                i.set((IdValuePair) o);
            }
        }

        @Override
        protected boolean hasNextImpl() {
            return cursor < pairs.size() || hasNull;
        }

        @Nullable
        @Override
        protected EntityId nextIdImpl() {
            if (cursor < pairs.size()) {
                final IdValuePair pair = pairs.get(cursor++);
                currentValue = pair.propValue;
                return new PersistentEntityId(sourceTypeId, pair.localId);
            } else {
                hasNull = false;
                currentValue = null;
                return null;
            }
        }

        @Nullable
        @Override
        public Comparable currentValue() {
            return currentValue;
        }
    }

    private static final class IdValuePair {

        final long localId;
        @Nullable
        final Comparable propValue;

        IdValuePair(final long localId, @Nullable final Comparable propValue) {
            this.localId = localId;
            this.propValue = propValue;
        }
    }

    private static final class PropertyValueIteratorFixingDecorator extends NonDisposableEntityIterator implements PropertyValueIterator {

        private final PropertyValueIterator index;
        private final EntityIteratorBase iterator;
        private boolean hasNext;
        private boolean hasNextValid;

        PropertyValueIteratorFixingDecorator(@NotNull final EntityIterableBase iterable,
                                             @NotNull final PropertyValueIterator index,
                                             @NotNull final EntityIteratorBase iterator) {
            super(iterable);
            this.index = index;
            this.iterator = iterator;
        }

        @Override
        protected boolean hasNextImpl() {
            if (!hasNextValid) {
                hasNext = iterator.hasNextImpl();
                hasNextValid = true;
            }
            return hasNext;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            final EntityId result = hasNextImpl() ? iterator.nextIdImpl() : null;
            hasNextValid = false;
            return result;
        }

        @Override
        public Comparable currentValue() {
            return index.currentValue();
        }
    }
}
