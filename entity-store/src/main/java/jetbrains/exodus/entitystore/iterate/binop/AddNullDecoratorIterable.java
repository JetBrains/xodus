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

import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.entitystore.iterate.EntityIterableInstantiator;
import jetbrains.exodus.entitystore.iterate.EntityIteratorBase;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.NoSuchElementException;

public class AddNullDecoratorIterable extends BinaryOperatorEntityIterable {

    static {
        registerType(EntityIterableType.ADD_NULL, new EntityIterableInstantiator() {
            @Override
            public EntityIterableBase instantiate(PersistentStoreTransaction txn, PersistentEntityStoreImpl store, Object[] parameters) {
                return new AddNullDecoratorIterable(txn, 
                        (EntityIterableBase) parameters[0], (EntityIterableBase) parameters[1]);
            }
        });
    }

    public AddNullDecoratorIterable(@NotNull final PersistentStoreTransaction txn,
                                    @NotNull final EntityIterableBase decorated,
                                    @NotNull final EntityIterableBase nullContainer) {
        super(txn, decorated, nullContainer, false);
        if (decorated.isSortedById()) {
            depth += SORTED_BY_ID_FLAG;
        }
    }

    @NotNull
    @Override
    public EntityIterator getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        return new EntityIteratorBase(AddNullDecoratorIterable.this) {
            private EntityIterator iterator = null;
            private Boolean hasNull = null;

            @Override
            public boolean hasNextImpl() {
                if (iterator == null) {
                    iterator = getLeft().iterator();
                }
                if (iterator.hasNext()) {
                    return true;
                }
                if (hasNull == null) {
                    hasNull = false;
                    for (Entity entity : getRight()) {
                        if (entity == null) {
                            hasNull = true;
                            break;
                        }
                    }
                }
                return hasNull;
            }

            @Override
            @Nullable
            protected EntityId nextIdImpl() {
                if (hasNextImpl()) {
                    if (hasNull == null) {
                        EntityId result = iterator.nextId();
                        if (result == null) {
                            hasNull = false;
                        }
                        return result;
                    } else {
                        hasNull = false;
                        return null;
                    }
                }
                throw new NoSuchElementException();
            }
        };
    }

    @Override
    protected EntityIterableType getIterableType() {
        return EntityIterableType.ADD_NULL;
    }
}
