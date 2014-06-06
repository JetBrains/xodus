/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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

    private final int entityTypeId;

    protected FilterEntityTypeIterable(@NotNull final PersistentEntityStoreImpl store,
                                       @NotNull final EntityIterableBase source,
                                       final int entityTypeId) {
        super(store, source);
        this.entityTypeId = entityTypeId;
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
        return new EntityIterableHandleDecorator(getStore(), EntityIterableType.FILTER_TYPE, source.getHandle()) {
            @Override
            public void getStringHandle(@NotNull final StringBuilder builder) {
                super.getStringHandle(builder);
                builder.append('-');
                builder.append(entityTypeId);
                builder.append('-');
                decorated.getStringHandle(builder);
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
