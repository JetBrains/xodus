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

import java.util.NoSuchElementException;

public class ExcludeNullIterableDecorator extends EntityIterableDecoratorBase {

    static {
        registerType(getType(), new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new ExcludeNullIterableDecorator(txn, (EntityIterableBase) parameters[0]);
            }
        });
    }

    public ExcludeNullIterableDecorator(@NotNull final PersistentStoreTransaction txn,
                                        @NotNull final EntityIterableBase source) {
        super(txn, source);
    }

    public static EntityIterableType getType() {
        return EntityIterableType.EXCLUDE_NULL;
    }

    @Override
    public boolean isSortedById() {
        return source.isSortedById();
    }

    @NotNull
    @Override
    public EntityIterator getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new EntityIteratorBase(ExcludeNullIterableDecorator.this) {
            private final EntityIterator iterator = getDecorated().iterator();
            private EntityId next = null;

            @Override
            public boolean hasNextImpl() {
                if (next != null) {
                    return true;
                }
                while (iterator.hasNext() && next == null) {
                    next = iterator.nextId();
                }
                return next != null;
            }

            @Override
            public EntityId nextIdImpl() {
                if (hasNext()) {
                    EntityId result = next;
                    next = null;
                    return result;
                }
                throw new NoSuchElementException();
            }
        };
    }

    @NotNull
    @Override
    protected EntityIterableHandle getHandleImpl() {
        return new EntityIterableHandleDecorator(getStore(), ExcludeNullIterableDecorator.getType(), source.getHandle()) {
            @Override
            public void toString(@NotNull final StringBuilder builder) {
                super.toString(builder);
                applyDecoratedToBuilder(builder);
            }
        };
    }
}
