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
import jetbrains.exodus.entitystore.util.EntityIdSetFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

public final class UnionIterable extends BinaryOperatorEntityIterable {

    static {
        registerType(EntityIterableType.UNION, new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new UnionIterable(txn,
                        (EntityIterableBase) parameters[0], (EntityIterableBase) parameters[1]);
            }
        });
    }

    public UnionIterable(@Nullable final PersistentStoreTransaction txn,
                         @NotNull final EntityIterableBase iterable1,
                         @NotNull final EntityIterableBase iterable2) {
        super(txn, iterable1, iterable2, true);
        if (iterable1.isSortedById() && iterable2.isSortedById()) {
            depth += SORTED_BY_ID_FLAG;
        }
    }

    @Override
    protected EntityIterableType getIterableType() {
        return EntityIterableType.UNION;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        EntityIterableBase iterable1 = this.iterable1;
        EntityIterableBase iterable2 = this.iterable2;
        if (isSortedById()) {
            return new EntityIteratorFixingDecorator(this, new SortedIterator(this, iterable1, iterable2));
        }
        if (iterable1.isSortedById()) {
            // swap iterables so that iterable1 is not sorted by id
            final EntityIterableBase temp = iterable1;
            iterable1 = iterable2;
            iterable2 = temp;
        }
        return new EntityIteratorFixingDecorator(this, new UnsortedIterator(txn, this, iterable1, iterable2));
    }

    private static final class SortedIterator extends NonDisposableEntityIterator {

        private EntityIteratorBase iterator1;
        private EntityIteratorBase iterator2;
        private EntityId e1;
        private EntityId e2;
        private EntityId nextId;

        private SortedIterator(@NotNull final EntityIterableBase iterable,
                               @NotNull final EntityIterableBase iterable1,
                               @NotNull final EntityIterableBase iterable2) {
            super(iterable);
            iterator1 = (EntityIteratorBase) iterable1.iterator();
            iterator2 = (EntityIteratorBase) iterable2.iterator();
            nextId = null;
            e1 = null;
            e2 = null;
        }

        @Override
        protected boolean hasNextImpl() {
            if (e1 == null && iterator1 != null) {
                if (iterator1.hasNext()) {
                    e1 = iterator1.nextId();
                } else {
                    iterator1 = null;
                }
            }
            if (e2 == null && iterator2 != null) {
                if (iterator2.hasNext()) {
                    e2 = iterator2.nextId();
                } else {
                    iterator2 = null;
                }
            }
            if (e1 == null) {
                nextId = e2;
                e2 = null;
            } else if (e2 == null) {
                nextId = e1;
                e1 = null;
            } else {
                final int cmp = e1.compareTo(e2);
                if (cmp < 0) {
                    nextId = e1;
                    e1 = null;
                } else if (cmp > 0) {
                    nextId = e2;
                    e2 = null;
                } else {
                    nextId = e1;
                    e1 = e2 = null;
                }
            }
            return nextId != null;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            final EntityId nextId = this.nextId;
            this.nextId = null;
            return nextId;
        }
    }

    private static final class UnsortedIterator extends NonDisposableEntityIterator {

        @NotNull
        private final PersistentStoreTransaction txn;
        @NotNull
        private final EntityIterableBase iterable1;
        @NotNull
        private final EntityIterableBase iterable2;
        private EntityIdSet iterated;
        private Iterator<EntityId> iterator1;
        private Iterator<EntityId> iterator2;
        private EntityId nextId;

        private UnsortedIterator(@NotNull final PersistentStoreTransaction txn,
                                 @NotNull final EntityIterableBase iterable,
                                 @NotNull final EntityIterableBase iterable1,
                                 @NotNull final EntityIterableBase iterable2) {
            super(iterable);
            this.txn = txn;
            if (iterable1.isSortedById()) {
                this.iterable1 = iterable1;
                this.iterable2 = iterable2;
            } else {
                this.iterable1 = iterable2;
                this.iterable2 = iterable1;
            }
            iterated = EntityIdSetFactory.newSet();
            nextId = PersistentEntityId.EMPTY_ID;
        }

        @Override
        protected boolean hasNextImpl() {
            if (nextId == PersistentEntityId.EMPTY_ID) {
                iterator1 = iterable1.isSortedById() ? toEntityIdIterator(iterable1.iterator()) : iterable1.toSet(txn).iterator();
            }
            if (iterator1 != null) {
                if (iterator1.hasNext()) {
                    nextId = iterator1.next();
                    return true;
                }
                iterator1 = null;
                iterator2 = iterable2.isSortedById() ? toEntityIdIterator(iterable2.iterator()) : iterable2.toSet(txn).iterator();
            }
            while (iterator2 != null && iterator2.hasNext()) {
                final EntityId nextId = iterator2.next();
                if (!iterated.contains(nextId)) {
                    this.nextId = nextId;
                    return true;
                }
            }
            iterator2 = null;
            return false;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            final EntityId nextId = this.nextId;
            iterated = iterated.add(nextId);
            return nextId;
        }

        @Nullable
        @Override
        protected EntityIdSet toSet() {
            return iterated;
        }
    }

    private static Iterator<EntityId> toEntityIdIterator(@NotNull final EntityIterator it) {
        return new Iterator<EntityId>() {

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public EntityId next() {
                return it.nextId();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}