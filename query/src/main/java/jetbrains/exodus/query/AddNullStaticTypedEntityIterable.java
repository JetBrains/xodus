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
package jetbrains.exodus.query;


import jetbrains.exodus.entitystore.Entity;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.entitystore.iterate.binop.AddNullDecoratorIterable;

import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * Iterates over decorated. Adds null to the end if nullContainer contains null.
 */
public class AddNullStaticTypedEntityIterable extends StaticTypedEntityIterable {
    private StaticTypedEntityIterable decorated;
    private StaticTypedEntityIterable nullContainer;

    public AddNullStaticTypedEntityIterable(String entityType, StaticTypedEntityIterable decorated,
                                            StaticTypedEntityIterable nullContainer, QueryEngine engine) {
        super(engine);
        this.entityType = entityType;
        this.decorated = decorated;
        this.nullContainer = nullContainer;
    }

    @Override
    public Iterable<Entity> instantiate() {
        Iterable<Entity> instantiatedDecorated = decorated.instantiate();
        Iterable<Entity> instantiatedNullContainer = nullContainer.instantiate();
        if (queryEngine.isPersistentIterable(instantiatedDecorated) && queryEngine.isPersistentIterable(instantiatedNullContainer)) {
            EntityIterableBase entityIterableBaseDecorated = ((EntityIterableBase) instantiatedDecorated).getSource();
            EntityIterableBase entityIterableBaseNullContainer = ((EntityIterableBase) instantiatedNullContainer).getSource();
            return queryEngine.wrap(new AddNullDecoratorIterable(
                    queryEngine.getPersistentStore().getAndCheckCurrentTransaction(), entityIterableBaseDecorated, entityIterableBaseNullContainer));
        }
        return new Iterable<Entity>() {
            @Override
            public Iterator<Entity> iterator() {
                return new Iterator<Entity>() {
                    private Iterator<Entity> iterator = null;
                    private Boolean hasNull = null;

                    @Override
                    public boolean hasNext() {
                        if (iterator == null) {
                            iterator = decorated.iterator();
                        }
                        if (iterator.hasNext()) {
                            return true;
                        }
                        if (hasNull == null) {
                            hasNull = false;
                            for (Entity entity : nullContainer) {
                                if (entity == null) {
                                    hasNull = true;
                                    break;
                                }
                            }
                        }
                        return hasNull;
                    }

                    @Override
                    public Entity next() {
                        if (hasNext()) {
                            if (hasNull == null) {
                                Entity result = iterator.next();
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

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }
}
