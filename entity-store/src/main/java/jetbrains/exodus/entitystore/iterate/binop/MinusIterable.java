/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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

public final class MinusIterable extends BinaryOperatorEntityIterable {

    static {
        EntityIterableBase.registerType(EntityIterableType.MINUS, (txn, store, parameters) -> new MinusIterable(txn,
            (EntityIterableBase) parameters[0], (EntityIterableBase) parameters[1]));
    }

    public MinusIterable(@Nullable final PersistentStoreTransaction txn,
                         @NotNull final EntityIterableBase minuend,
                         @NotNull final EntityIterableBase subtrahend) {
        super(txn, minuend, subtrahend, false);
        // minuend is always equal to iterable1, 'cause we are not commutative
        if (minuend.isSortedById()) {
            depth += SORTED_BY_ID_FLAG;
        }
    }

    @Override
    protected EntityIterableType getIterableType() {
        return EntityIterableType.MINUS;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        final EntityIterableBase iterable1 = this.iterable1;
        final EntityIterableBase iterable2 = this.iterable2;
        return new EntityIteratorFixingDecorator(this, isSortedById() && iterable2.isSortedById() ?
            new SortedIterator(this, iterable1, iterable2) : new UnsortedIterator(this, txn, iterable1, iterable2));
    }

    @Override
    public @NotNull EntityIterator getReverseIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        if (isSortedById()) {
            return new EntityIteratorFixingDecorator(this,
                new SortedReverseIterator(this, txn, iterable1, iterable2));
        }
        return super.getReverseIteratorImpl(txn);
    }

    private static abstract class SortedIteratorBase extends NonDisposableEntityIterator {

        protected EntityId nextId;
        protected EntityId currentMinuend;
        protected EntityId currentSubtrahend;

        protected SortedIteratorBase(@NotNull final EntityIterableBase iterable) {
            super(iterable);
            nextId = null;
            currentMinuend = null;
            currentSubtrahend = null;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            return nextId;
        }
    }

    private static final class SortedIterator extends SortedIteratorBase {

        private final EntityIteratorBase minuend;
        private final EntityIteratorBase subtrahend;

        SortedIterator(@NotNull final EntityIterableBase iterable,
                       @NotNull final EntityIterableBase minuend,
                       @NotNull final EntityIterableBase subtrahend) {
            super(iterable);
            this.minuend = (EntityIteratorBase) minuend.iterator();
            this.subtrahend = (EntityIteratorBase) subtrahend.iterator();
        }

        @Override
        protected boolean hasNextImpl() {
            EntityId currentMinuend = this.currentMinuend;
            EntityId currentSubtrahend = this.currentSubtrahend;
            while (currentMinuend != PersistentEntityId.EMPTY_ID) {
                if (currentMinuend == null) {
                    this.currentMinuend = currentMinuend = checkNextId(minuend);
                }
                if (currentSubtrahend == null) {
                    this.currentSubtrahend = currentSubtrahend = checkNextId(subtrahend);
                }
                // no more ids in minuend
                if ((nextId = currentMinuend) == PersistentEntityId.EMPTY_ID) {
                    break;
                }
                // no more ids subtrahend
                if (currentSubtrahend == PersistentEntityId.EMPTY_ID) {
                    currentMinuend = null;
                    break;
                }
                if (currentMinuend != currentSubtrahend && (currentMinuend == null || currentSubtrahend == null)) {
                    break;
                }
                if (currentMinuend == currentSubtrahend) {
                    currentMinuend = currentSubtrahend = null;
                    continue;
                }
                final int cmp = currentMinuend.compareTo(currentSubtrahend);
                if (cmp < 0) {
                    currentMinuend = null;
                    break;
                }
                currentSubtrahend = null;
                if (cmp == 0) {
                    currentMinuend = null;
                }
            }
            this.currentMinuend = currentMinuend;
            this.currentSubtrahend = currentSubtrahend;
            return this.currentMinuend != PersistentEntityId.EMPTY_ID;
        }
    }

    private static final class SortedReverseIterator extends SortedIteratorBase {

        private final EntityIteratorBase minuend;
        private final EntityIteratorBase subtrahend;


        SortedReverseIterator(@NotNull final EntityIterableBase iterable,
                              @NotNull final PersistentStoreTransaction txn,
                              @NotNull final EntityIterableBase minuend,
                              @NotNull final EntityIterableBase subtrahend) {
            super(iterable);
            this.minuend = (EntityIteratorBase) minuend.getReverseIteratorImpl(txn);
            this.subtrahend = (EntityIteratorBase) subtrahend.getReverseIteratorImpl(txn);
        }

        @Override
        protected boolean hasNextImpl() {
            EntityId currentMinuend = this.currentMinuend;
            EntityId currentSubtrahend = this.currentSubtrahend;
            while (currentMinuend != PersistentEntityId.EMPTY_ID) {
                if (currentMinuend == null) {
                    this.currentMinuend = currentMinuend = checkNextId(minuend);
                }
                if (currentSubtrahend == null) {
                    this.currentSubtrahend = currentSubtrahend = checkNextId(subtrahend);
                }
                // no more ids in minuend
                if ((nextId = currentMinuend) == PersistentEntityId.EMPTY_ID) {
                    break;
                }
                // no more ids subtrahend
                if (currentSubtrahend == PersistentEntityId.EMPTY_ID) {
                    currentMinuend = null;
                    break;
                }
                if (currentMinuend != currentSubtrahend && (currentMinuend == null || currentSubtrahend == null)) {
                    break;
                }
                if (currentMinuend == currentSubtrahend) {
                    currentMinuend = currentSubtrahend = null;
                    continue;
                }
                final int cmp = currentMinuend.compareTo(currentSubtrahend);
                if (cmp > 0) {
                    currentMinuend = null;
                    break;
                }
                currentSubtrahend = null;
                if (cmp == 0) {
                    currentMinuend = null;
                }
            }
            this.currentMinuend = currentMinuend;
            this.currentSubtrahend = currentSubtrahend;
            return this.currentMinuend != PersistentEntityId.EMPTY_ID;
        }
    }

    @Nullable
    private static EntityId checkNextId(final EntityIterator it) {
        return it.hasNext() ? it.nextId() : PersistentEntityId.EMPTY_ID;
    }

    private static final class UnsortedIterator extends NonDisposableEntityIterator {

        @NotNull
        private final PersistentStoreTransaction txn;
        @NotNull
        private final EntityIteratorBase minuend;
        private EntityIterableBase subtrahend;
        private EntityIdSet exceptSet;
        private EntityId nextId;

        private UnsortedIterator(@NotNull final EntityIterableBase iterable,
                                 @NotNull final PersistentStoreTransaction txn,
                                 @NotNull final EntityIterableBase minuend,
                                 @NotNull final EntityIterableBase subtrahend) {
            super(iterable);
            this.txn = txn;
            this.minuend = (EntityIteratorBase) minuend.iterator();
            this.subtrahend = subtrahend;
        }

        @Override
        protected boolean hasNextImpl() {
            while (minuend.hasNext()) {
                final EntityId nextId = minuend.nextId();
                if (!getExceptSet().contains(nextId)) {
                    this.nextId = nextId;
                    return true;
                }
            }
            nextId = null;
            return false;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            return nextId;
        }

        private EntityIdSet getExceptSet() {
            if (exceptSet == null) {
                exceptSet = subtrahend.toSet(txn);
                // reclaim memory as early as possible
                subtrahend = null;
            }
            return exceptSet;
        }
    }
}
