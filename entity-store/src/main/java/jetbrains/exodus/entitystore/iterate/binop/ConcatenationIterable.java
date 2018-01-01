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

import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.EntityIterableType;
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.entitystore.iterate.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class ConcatenationIterable extends BinaryOperatorEntityIterable {

    static {
        registerType(EntityIterableType.CONCAT, new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new ConcatenationIterable(txn,
                        (EntityIterableBase) parameters[0], (EntityIterableBase) parameters[1]);
            }
        });
    }

    public ConcatenationIterable(@Nullable final PersistentStoreTransaction txn,
                                 @NotNull final EntityIterableBase iterable1,
                                 @NotNull final EntityIterableBase iterable2) {
        super(txn, iterable1, iterable2, false);
    }

    @Override
    public long size() {
        return iterable1.size() + iterable2.size();
    }

    @Override
    public boolean isSortedById() {
        return false;
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return iterable1.size() + iterable2.size();
    }

    @Override
    protected EntityIterableType getIterableType() {
        return EntityIterableType.CONCAT;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new EntityIteratorFixingDecorator(this, new ConcatenationIterator(iterable1, iterable2));
    }

    private final class ConcatenationIterator extends NonDisposableEntityIterator {

        private final EntityIterableBase iterable1;
        private final EntityIterableBase iterable2;
        private EntityIteratorBase iterator1;
        private EntityIteratorBase iterator2;

        private ConcatenationIterator(@NotNull final EntityIterableBase iterable1,
                                      @NotNull final EntityIterableBase iterable2) {
            super(ConcatenationIterable.this);
            this.iterable1 = iterable1;
            this.iterable2 = iterable2;
            iterator1 = this;
            iterator2 = this;
        }

        @Override
        protected boolean hasNextImpl() {
            if (iterator1 == this) {
                iterator1 = (EntityIteratorBase) iterable1.iterator();
            }
            if (iterator1 != null) {
                if (iterator1.hasNext()) {
                    return true;
                }
                iterator1 = null;
                iterator2 = (EntityIteratorBase) iterable2.iterator();
            }
            if (iterator2 != null) {
                if (iterator2.hasNext()) {
                    return true;
                }
                iterator2 = null;
            }
            return false;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            if (iterator1 != null) {
                return iterator1.nextId();
            }
            if (iterator2 != null) {
                return iterator2.nextId();
            }
            return null;
        }
    }
}
