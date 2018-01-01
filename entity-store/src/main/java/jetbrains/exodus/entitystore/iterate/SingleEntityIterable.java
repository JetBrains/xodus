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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SingleEntityIterable extends EntityIterableBase {

    @Nullable
    private final EntityId id;

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new SingleEntityIterable(txn,
                        new PersistentEntityId(Integer.valueOf((String) parameters[0]),
                                Integer.valueOf((String) parameters[1]))
                );
            }
        });
    }

    public SingleEntityIterable(@Nullable final PersistentStoreTransaction txn, @Nullable final EntityId id) {
        super(txn);
        this.id = id;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.SINGLE_ENTITY;
    }

    @Override
    public long size() {
        return 1;
    }

    @Override
    public long count() {
        return 1;
    }

    @Override
    public long getRoughCount() {
        return 1;
    }

    @Override
    public int indexOf(@NotNull final Entity entity) {
        return indexOfImpl(entity.getId());
    }

    @Override
    public boolean canBeCached() {
        return false;
    }

    @Override
    protected int indexOfImpl(@NotNull final EntityId entityId) {
        final EntityId id = this.id;
        return id != null && id.equals(entityId) ? 0 : -1;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new SingleEntityIterator(this);
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new EntityIterableHandleBase(getStore(), SingleEntityIterable.getType()) {

            @NotNull
            @Override
            public int[] getLinkIds() {
                return IdFilter.EMPTY_ID_ARRAY;
            }

            @Override
            public boolean isMatchedLinkAdded(@NotNull EntityId source, @NotNull EntityId target, int linkId) {
                return false;
            }

            @Override
            public boolean isMatchedLinkDeleted(@NotNull EntityId source, @NotNull EntityId target, int linkId) {
                return false;
            }

            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                final EntityId id = SingleEntityIterable.this.id;
                if (id == null) {
                    builder.append("null");
                } else {
                    ((PersistentEntityId) id).toString(builder);
                }
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                final EntityId id = SingleEntityIterable.this.id;
                if (id == null) {
                    hash.apply("null");
                } else {
                    ((PersistentEntityId) id).toHash(hash);
                }
            }
        };
    }

    private class SingleEntityIterator extends NonDisposableEntityIterator {

        private boolean hasNext;

        private SingleEntityIterator(@NotNull final EntityIterableBase iterable) {
            super(iterable);
            hasNext = true;
        }

        @Override
        protected boolean hasNextImpl() {
            return hasNext;
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            final EntityId id = (hasNext) ? SingleEntityIterable.this.id : null;
            hasNext = false;
            return id;
        }
    }
}
