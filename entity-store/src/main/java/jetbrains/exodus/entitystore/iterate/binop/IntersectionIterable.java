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
package jetbrains.exodus.entitystore.iterate.binop;

import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.iterate.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class IntersectionIterable extends BinaryOperatorEntityIterable {

    static {
        registerType(EntityIterableType.INTERSECT, new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new IntersectionIterable(txn,
                        (EntityIterableBase) parameters[0], (EntityIterableBase) parameters[1]);
            }
        });
    }

    public IntersectionIterable(@Nullable final PersistentStoreTransaction txn,
                                @NotNull final EntityIterableBase iterable1,
                                @NotNull final EntityIterableBase iterable2) {
        this(txn, iterable1, iterable2, false);
    }

    public IntersectionIterable(@Nullable final PersistentStoreTransaction txn,
                                @NotNull final EntityIterableBase iterable1,
                                @NotNull final EntityIterableBase iterable2,
                                final boolean preserveRightOrder) {
        super(txn, iterable1, iterable2, !preserveRightOrder);
        if (preserveRightOrder) {
            if (this.iterable2.isSortedById()) {
                depth += SORTED_BY_ID_FLAG;
            }
        } else {
            if (iterable1.isSortedById() || iterable2.isSortedById()) {
                depth += SORTED_BY_ID_FLAG;
            }
        }
    }

    @Override
    protected EntityIterableType getIterableType() {
        return EntityIterableType.INTERSECT;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        final EntityIterableBase iterable1 = this.iterable1;
        final EntityIterableBase iterable2 = this.iterable2;
        final EntityIteratorBase iterator;
        if (isSortedById()) {
            if (iterable1.isSortedById()) {
                iterator = iterable2.isSortedById() ?
                        new SortedIterator(this, iterable1, iterable2) :
                        new UnsortedIterator(this, txn, iterable2, iterable1);
            } else {
                // iterable2 is sorted for sure
                iterator = new UnsortedIterator(this, txn, iterable1, iterable2);
            }
        } else {
            // both unsorted or order preservation needed
            iterator = new UnsortedIterator(this, txn, iterable1, iterable2);
        }
        return new EntityIteratorFixingDecorator(this, iterator);
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return isEmptyFast(txn) ? 0 : super.countImpl(txn);
    }

    @Override
    public boolean isEmptyImpl(@NotNull final PersistentStoreTransaction txn) {
        return isEmptyFast(txn) || super.isEmptyImpl(txn);
    }

    @Override
    protected boolean isEmptyFast(@NotNull final PersistentStoreTransaction txn) {
        return super.isEmptyFast(txn) ||
                ((iterable1.isCached() || iterable1.nonCachedHasFastCountAndIsEmpty()) && iterable1.isEmptyImpl(txn)) ||
                ((iterable2.isCached() || iterable2.nonCachedHasFastCountAndIsEmpty()) && iterable2.isEmptyImpl(txn));
    }

    private static final class SortedIterator extends NonDisposableEntityIterator {

        @NotNull
        private final EntityIteratorBase iterator1;
        @NotNull
        private final EntityIteratorBase iterator2;
        private EntityId nextId;

        private SortedIterator(@NotNull final EntityIterableBase iterable,
                               @NotNull final EntityIterableBase iterable1,
                               @NotNull final EntityIterableBase iterable2) {
            super(iterable);
            iterator1 = (EntityIteratorBase) iterable1.iterator();
            iterator2 = (EntityIteratorBase) iterable2.iterator();
            nextId = null;
        }

        @Override
        protected boolean hasNextImpl() {
            EntityId next = nextId;
            if (next != PersistentEntityId.EMPTY_ID) {
                next = PersistentEntityId.EMPTY_ID;
                EntityId e1 = null;
                EntityId e2 = null;
                final EntityIteratorBase iterator1 = this.iterator1;
                final EntityIteratorBase iterator2 = this.iterator2;
                while (true) {
                    if (e1 == null) {
                        if (!iterator1.hasNext()) {
                            break;
                        }
                        e1 = iterator1.nextId();
                    }
                    if (e2 == null) {
                        if (!iterator2.hasNext()) {
                            break;
                        }
                        e2 = iterator2.nextId();
                    }
                    // check if single id is null, not both
                    if (e1 != e2 && (e1 == null || e2 == null)) {
                        continue;
                    }
                    final int cmp = e1 == e2 ? 0 : e1.compareTo(e2);
                    if (cmp < 0) {
                        e1 = null;
                    } else if (cmp > 0) {
                        e2 = null;
                    } else {
                        next = e1;
                        break;
                    }
                }
                return (nextId = next) != PersistentEntityId.EMPTY_ID;
            }
            return false;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            return nextId;
        }
    }

    private static final class UnsortedIterator extends NonDisposableEntityIterator {

        @NotNull
        private final PersistentStoreTransaction txn;
        private EntityIterableBase iterable1;
        @NotNull
        private final EntityIteratorBase iterator2;
        private EntityIdSet entityIdSet;
        private EntityId nextId;

        private UnsortedIterator(@NotNull final EntityIterableBase iterable,
                                 @NotNull final PersistentStoreTransaction txn,
                                 @NotNull EntityIterableBase iterable1,
                                 @NotNull EntityIterableBase iterable2) {
            super(iterable);
            this.txn = txn;
            this.iterable1 = iterable1;
            iterator2 = (EntityIteratorBase) iterable2.iterator();
            nextId = null;
        }

        @Override
        protected boolean hasNextImpl() {
            while (iterator2.hasNext()) {
                final EntityId nextId = iterator2.nextId();
                if (getEntityIdSet().contains(nextId)) {
                    this.nextId = nextId;
                    return true;
                }
            }
            return false;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            return nextId;
        }

        private EntityIdSet getEntityIdSet() {
            if (entityIdSet == null) {
                entityIdSet = iterable1.toSet(txn);
                // reclaim memory as early as possible
                iterable1 = null;
            }
            return entityIdSet;
        }

    }
}
