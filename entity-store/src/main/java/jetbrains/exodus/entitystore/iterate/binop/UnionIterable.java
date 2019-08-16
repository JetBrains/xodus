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
package jetbrains.exodus.entitystore.iterate.binop;

import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.iterate.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Comparator;
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
        // UnionIterable is always sorted by id
        depth += SORTED_BY_ID_FLAG;
    }

    @Override
    protected EntityIterableType getIterableType() {
        return EntityIterableType.UNION;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new EntityIteratorFixingDecorator(this, new SortedIterator(this, iterable1, iterable2));
    }

    private static final class SortedIterator extends NonDisposableEntityIterator {

        private Iterator<EntityId> iterator1;
        private Iterator<EntityId> iterator2;
        private EntityId e1;
        private EntityId e2;
        private EntityId nextId;

        private SortedIterator(@NotNull final EntityIterableBase iterable,
                               @NotNull final EntityIterableBase iterable1,
                               @NotNull final EntityIterableBase iterable2) {
            super(iterable);
            iterator1 = iterable1.isSortedById() ? toEntityIdIterator(iterable1.iterator()) : toSortedEntityIdIterator(iterable1.iterator());
            iterator2 = iterable2.isSortedById() ? toEntityIdIterator(iterable2.iterator()) : toSortedEntityIdIterator(iterable2.iterator());
            nextId = null;
            e1 = null;
            e2 = null;
        }

        @Override
        protected boolean hasNextImpl() {
            if (e1 == null && iterator1 != null) {
                if (iterator1.hasNext()) {
                    e1 = iterator1.next();
                } else {
                    iterator1 = null;
                }
            }
            if (e2 == null && iterator2 != null) {
                if (iterator2.hasNext()) {
                    e2 = iterator2.next();
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

    private static Iterator<EntityId> toSortedEntityIdIterator(@NotNull final EntityIterator it) {
        EntityId[] array = new EntityId[8];
        int i = 0;
        while (it.hasNext()) {
            if (i == array.length) {
                array = Arrays.copyOf(array, i * 2);
            }
            array[i++] = it.nextId();
        }
        final int size = i;
        final EntityId[] result = array;
        if (size > 1) {
            Arrays.sort(result, 0, size, new Comparator<EntityId>() {
                @Override
                public int compare(EntityId o1, EntityId o2) {
                    if (o1 == null) {
                        return 1;
                    }
                    if (o2 == null) {
                        return -1;
                    }
                    return o1.compareTo(o2);
                }
            });
        }
        return new Iterator<EntityId>() {
            int i = 0;

            @Override
            public boolean hasNext() {
                return i < size;
            }

            @Override
            public EntityId next() {
                return result[i++];
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}