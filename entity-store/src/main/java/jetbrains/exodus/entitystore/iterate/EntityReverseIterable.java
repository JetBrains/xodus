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

public final class EntityReverseIterable extends EntityIterableDecoratorBase {

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new EntityReverseIterable(store, (EntityIterableBase) parameters[0]);
            }
        });
    }

    public EntityReverseIterable(@NotNull final PersistentEntityStoreImpl store,
                                 @NotNull final EntityIterableBase source) {
        super(store, source);
    }

    public static EntityIterableType getType() {
        return EntityIterableType.REVERSE;
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
    @NotNull
    public EntityIterator getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return source.getOrCreateCachedWrapper(txn).getReverseIteratorImpl(txn);
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new EntityIterableHandleDecorator(getStore(), EntityReverseIterable.getType(), source.getHandle()) {
            @Override
            public void getStringHandle(@NotNull final StringBuilder builder) {
                super.getStringHandle(builder);
                builder.append('-');
                decorated.getStringHandle(builder);
            }
        };
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return source.size();
    }
}
