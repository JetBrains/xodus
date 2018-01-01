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
import jetbrains.exodus.entitystore.iterate.ExcludeNullIterableDecorator;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ExcludeNullStaticTypedEntityIterable extends StaticTypedEntityIterable {
    private final StaticTypedEntityIterable decorated;

    public ExcludeNullStaticTypedEntityIterable(String entityType, StaticTypedEntityIterable decorated,
                                                @NotNull final QueryEngine queryEngine) {
        super(queryEngine);
        this.entityType = entityType;
        this.decorated = decorated;
    }

    @Override
    public Iterable<Entity> instantiate() {
        Iterable<Entity> instantiatedDecorated = decorated.instantiate();
        if (queryEngine.isPersistentIterable(instantiatedDecorated)) {
            EntityIterableBase entityIterableBaseDecorated = ((EntityIterableBase) instantiatedDecorated).getSource();
            if (entityIterableBaseDecorated == EntityIterableBase.EMPTY) {
                return EntityIterableBase.EMPTY;
            }
            return queryEngine.wrap(new ExcludeNullIterableDecorator(entityIterableBaseDecorated.getTransaction(), entityIterableBaseDecorated));
        }
        return new Iterable<Entity>() {
            @Override
            public Iterator<Entity> iterator() {
                return new Iterator<Entity>() {
                    private Iterator<Entity> iterator = null;
                    private Entity next = null;

                    @Override
                    public boolean hasNext() {
                        if (next != null) {
                            return true;
                        }
                        if (iterator == null) {
                            iterator = decorated.iterator();
                        }
                        while (iterator.hasNext() && next == null) {
                            next = iterator.next();
                        }
                        return next != null;
                    }

                    @Override
                    public Entity next() {
                        if (hasNext()) {
                            Entity result = next;
                            next = null;
                            return result;
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
