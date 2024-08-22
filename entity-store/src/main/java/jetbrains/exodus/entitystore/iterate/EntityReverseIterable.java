/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.entitystore.EntityIterableHandle;
import jetbrains.exodus.entitystore.EntityIterableType;
import jetbrains.exodus.entitystore.EntityIterator;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import org.jetbrains.annotations.NotNull;

public final class EntityReverseIterable extends EntityIterableDecoratorBase {

    static {
        registerType(getType(), (txn, store, parameters) -> new EntityReverseIterable(txn, (EntityIterableBase) parameters[0]));
    }

    public EntityReverseIterable(@NotNull final PersistentStoreTransaction txn,
                                 @NotNull final EntityIterableBase source) {
        super(txn, source);
    }

    public static EntityIterableType getType() {
        return EntityIterableType.REVERSE;
    }

    @Override
    public int getEntityTypeId() {
        return source.getEntityTypeId();
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
    public long getRoughCount() {
        return source.getRoughCount();
    }

    @Override
    public long getRoughSize() {
        return source.getRoughSize();
    }

    @Override
    @NotNull
    public EntityIterator getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        try {
            return source.asProbablyCached().getReverseIteratorImpl(txn);
        } catch (UnsupportedOperationException ignore) {
        }
        return source.getOrCreateCachedInstance(txn).getReverseIteratorImpl(txn);
    }

    @Override
    public @NotNull EntityIterator getReverseIteratorImpl(@NotNull PersistentStoreTransaction txn) {
        return source.getIteratorImpl(txn);
    }

    @Override
    @NotNull
    protected EntityIterableHandle getHandleImpl() {
        return new EntityIterableHandleDecorator(getStore(), EntityReverseIterable.getType(), source.getHandle()) {
            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                applyDecoratedToBuilder(builder);
            }

            @Override
            public int getEntityTypeId() {
                return source.getEntityTypeId();
            }
        };
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return source.size();
    }
}
