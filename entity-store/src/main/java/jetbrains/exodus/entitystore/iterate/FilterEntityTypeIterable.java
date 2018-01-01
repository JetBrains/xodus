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

class FilterEntityTypeIterable extends EntityIterableDecoratorBase {

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new FilterEntityTypeIterable(txn,
                        Integer.valueOf((String) parameters[0]), (EntityIterableBase) parameters[1]);
            }
        });
    }

    private final int entityTypeId;

    protected FilterEntityTypeIterable(@NotNull final PersistentStoreTransaction txn,
                                       final int entityTypeId,
                                       @NotNull final EntityIterableBase source) {
        super(txn, source);
        this.entityTypeId = entityTypeId;
    }

    public static EntityIterableType getType() {
        return EntityIterableType.FILTER_ENTITY_TYPE;
    }

    public int getEntityTypeId() {
        return entityTypeId;
    }

    @NotNull
    @Override
    public EntityIterator getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new EntityIteratorFixingDecorator(this, new NonDisposableEntityIterator(this) {

            @NotNull
            private final EntityIteratorBase sourceIt = (EntityIteratorBase) source.iterator();
            @Nullable
            private EntityId nextId = PersistentEntityId.EMPTY_ID;

            @Override
            protected boolean hasNextImpl() {
                if (nextId != PersistentEntityId.EMPTY_ID) {
                    return true;
                }
                while (sourceIt.hasNext()) {
                    nextId = sourceIt.nextId();
                    if (nextId == null || nextId.getTypeId() == entityTypeId) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            protected EntityId nextIdImpl() {
                final EntityId result = nextId;
                nextId = PersistentEntityId.EMPTY_ID;
                return result;
            }
        });
    }

    @NotNull
    @Override
    protected EntityIterableHandle getHandleImpl() {
        return new EntityIterableHandleDecorator(getStore(), getType(), source.getHandle()) {

            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                builder.append(entityTypeId);
                builder.append('-');
                applyDecoratedToBuilder(builder);
            }

            @Override
            public void hashCode(@NotNull final EntityIterableHandleHash hash) {
                hash.apply(entityTypeId);
                hash.applyDelimiter();
                super.hashCode(hash);
            }

            @Override
            public int getEntityTypeId() {
                return entityTypeId;
            }
        };
    }

    @Override
    public boolean isSortedById() {
        return source.isSortedById();
    }

    @Override
    public boolean canBeCached() {
        return false;
    }
}
