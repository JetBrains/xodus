/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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

import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.LongHashSet;
import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import jetbrains.exodus.core.dataStructures.hash.LongSet;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.util.EntityIdSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;

@SuppressWarnings({"unchecked", "rawtypes"})
public final class SortIterable extends EntityIterableDecoratorBase {

    @NotNull
    private final EntityIterableBase propIndex;
    private final int sourceTypeId;
    private final boolean ascending;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new SortIterable(store, (EntityIterableBase) parameters[2],
                        (EntityIterableBase) parameters[3], Integer.valueOf((String) parameters[0]), "0".equals(parameters[1]));
            }
        });
    }

    public SortIterable(@NotNull final PersistentEntityStoreImpl store,
                        @NotNull final EntityIterableBase propIndex,
                        @NotNull final EntityIterableBase source,
                        final int sourceTypeId,
                        final boolean ascending) {
        super(store, source);
        this.propIndex = propIndex;
        this.sourceTypeId = sourceTypeId;
        this.ascending = ascending;
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
        final EntityIterableBase cached = store.getEntityIterableCache().putIfNotCached(propIndex);
        final EntityIterator propIterator = ascending ? cached.getIteratorImpl(txn) : cached.getReverseIteratorImpl(txn);
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

            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                builder.append(sourceTypeId);
                builder.append('-');
                applyDecoratedToBuilder(builder);
                builder.append('-');
                ((EntityIterableHandleBase) propIndex.getHandle()).toString(builder);
                builder.append('-');
                builder.append(ascending ? 0 : 1);
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                hash.apply(sourceTypeId);
                hash.applyDelimiter();
                super.hashCode(hash);
                hash.applyDelimiter();
                hash.apply(propIndex.getHandle());
                hash.applyDelimiter();
                hash.apply(ascending ? 0 : 1);
            }

            @Override
            public boolean isMatchedPropertyChanged(final int typeId,
                                                    final int propertyId,
                                                    @Nullable final Comparable oldValue,
                                                    @Nullable final Comparable newValue) {
                return decorated.isMatchedPropertyChanged(typeId, propertyId, oldValue, newValue) ||
                        propIndex.getHandle().isMatchedPropertyChanged(typeId, propertyId, oldValue, newValue);
            }
        };
    }

    @Override
    public boolean canBeCached() {
        return super.canBeCached() && propIndex.canBeCached();
    }

    @SuppressWarnings({"MethodOnlyUsedFromInnerClass"})
    @NotNull
    private Map<EntityId, Integer> getRightOrder() {
        final Map<EntityId, Integer> result = new HashMap<>();
        int position = 0;
        final EntityIterator sorted = source.iterator();
        while (sorted.hasNext()) {
            final EntityId entityId = sorted.nextId();
            if (entityId == null || sourceTypeId == entityId.getTypeId()) {
                result.put(entityId, ++position);
            }
        }
        return result;
    }

    private final class StableSortIterator extends NonDisposableEntityIterator implements PropertyValueIterator {

        @NotNull
        private final PropertyValueIterator propertyValueIterator;
        private final PriorityQueue<EntityId> sameValueQueue;
        private final Map<EntityId, Integer> rightOrder;
        private EntityId nextId;
        private Comparable currentValue;
        private Comparable lastValue;
        private EntityId lastEntityId;

        private StableSortIterator(@NotNull final PropertyValueIterator propertyValueIterator) {
            super(propIndex);
            this.propertyValueIterator = propertyValueIterator;
            sameValueQueue = new PriorityQueue<>(4, new Comparator<EntityId>() {
                @Override
                public int compare(final EntityId o1, final EntityId o2) {
                    return rightOrder.get(o1) - rightOrder.get(o2);
                }
            });
            rightOrder = getRightOrder();
            nextId = null;
            currentValue = null;
            lastValue = null;
            lastEntityId = null;
        }

        @SuppressWarnings({"OverlyLongMethod"})
        @Override
        protected boolean hasNextImpl() {
            final PriorityQueue<EntityId> sameValueQueue = this.sameValueQueue;
            final Map<EntityId, Integer> rightOrder = this.rightOrder;
            if (sameValueQueue.isEmpty()) {
                Comparable lastValue = this.lastValue;
                //noinspection VariableNotUsedInsideIf
                if (lastValue != null) {
                    currentValue = lastValue;
                    sameValueQueue.offer(lastEntityId);
                    this.lastValue = null;
                    lastEntityId = null;
                }
                while (propertyValueIterator.hasNext()) {
                    final EntityId nextId = propertyValueIterator.nextId();
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
                    final EntityId id = sameValueQueue.poll();
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
                for (EntityId entityId : rightOrder.keySet()) {
                    sameValueQueue.offer(entityId);
                }
            }
            if (rightOrder.containsKey(null)) {
                rightOrder.remove(null);
                nextId = null;
                return true;
            }
            return false;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            return nextId;
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
            final LongSet rightOrder = sourceSet.getTypeSet(sourceTypeId);
            if (rightOrder == null) {
                this.rightOrder = LongSet.EMPTY;
            } else {
                this.rightOrder = new LongHashSet(rightOrder.size());
                for (final long localId : rightOrder) {
                    this.rightOrder.add(localId);
                }
            }
            nextId = null;
        }

        @Override
        protected boolean hasNextImpl() {
            final LongSet rightOrder = this.rightOrder;
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

    private static final class PropertyValueIteratorFixingDecorator extends NonDisposableEntityIterator implements PropertyValueIterator {

        private final PropertyValueIterator index;
        private final EntityIteratorBase iterator;
        private boolean hasNext;
        private boolean hasNextValid;

        public PropertyValueIteratorFixingDecorator(@NotNull final EntityIterableBase iterable,
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
