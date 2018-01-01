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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.util.EntityIdSetFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class DistinctIterable extends EntityIterableDecoratorBase {

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new DistinctIterable(txn, (EntityIterableBase) parameters[0]);
            }
        });
    }

    public DistinctIterable(@NotNull final PersistentStoreTransaction txn,
                            @NotNull final EntityIterableBase source) {
        super(txn, source);
    }

    public static EntityIterableType getType() {
        return EntityIterableType.DISTINCT;
    }

    @Override
    public boolean isSortedById() {
        return source.isSortedById();
    }

    // canBeReordered is true because sorted iterator is already sorted and unsorted iterator actually performs reordering

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new EntityIteratorFixingDecorator(this, isSortedById() ?
                new DistinctSortedIterator(this, source) :
                new DistinctUnsortedIterator(this, source));
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new EntityIterableHandleDecorator(getStore(), DistinctIterable.getType(), source.getHandle()) {
            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                applyDecoratedToBuilder(builder);
            }
        };
    }

    private static final class DistinctSortedIterator extends NonDisposableEntityIterator {

        @NotNull
        private final EntityIterator source;
        @Nullable
        private EntityId nextId;

        private DistinctSortedIterator(@NotNull final DistinctIterable iterable,
                                       @NotNull final EntityIterable source) {
            super(iterable);
            this.source = source.iterator();
        }

        @Override
        protected boolean hasNextImpl() {
            while (source.hasNext()) {
                final EntityId id = source.nextId();
                if (nextId == null) {
                    if (id != null) {
                        nextId = id;
                        return true;
                    }
                } else {
                    if (id == null || !id.equals(nextId)) {
                        nextId = id;
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            return nextId;
        }
    }

    private static final class DistinctUnsortedIterator extends NonDisposableEntityIterator {

        @NotNull
        private final EntityIterator source;
        @NotNull
        private EntityIdSet iterated;
        @Nullable
        private EntityId nextId;

        private DistinctUnsortedIterator(@NotNull final DistinctIterable iterable,
                                         @NotNull final EntityIterable source) {
            super(iterable);
            this.source = source.iterator();
            iterated = EntityIdSetFactory.newSet();
        }

        @Override
        protected boolean hasNextImpl() {
            while (source.hasNext()) {
                final EntityId id = source.nextId();
                if (!iterated.contains(id)) {
                    nextId = id;
                    iterated = iterated.add(id);
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

        @Nullable
        @Override
        protected EntityIdSet toSet() {
            return iterated;
        }
    }
}
