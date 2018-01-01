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
import jetbrains.exodus.entitystore.EntityIterable;
import jetbrains.exodus.entitystore.EntityIterator;
import jetbrains.exodus.entitystore.EntityStoreException;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.query.metadata.ModelMetaData;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

@SuppressWarnings("SpellCheckingInspection")
public class And extends CommutativeOperator {

    private static final boolean traceFindLinks = Boolean.getBoolean("jetbrains.exodus.query.traceFindLinks");

    public And(final NodeBase left, final NodeBase right) {
        super(left, right);
    }

    @Override
    public Iterable<Entity> instantiate(final String entityType, final QueryEngine queryEngine, final ModelMetaData metaData) {
        final NodeBase left = getLeft();
        final NodeBase right = getRight();
        final Instantiatable directClosure = new Instantiatable() {
            @Override
            public Iterable<Entity> instantiate() {
                return queryEngine.intersectAdjusted(left.instantiate(entityType, queryEngine, metaData), right.instantiate(entityType, queryEngine, metaData));
            }
        };
        if (left instanceof LinksEqualDecorator) {
            return instantiateCustom(entityType, queryEngine, metaData, right, (LinksEqualDecorator) left, directClosure);
        }
        if (right instanceof LinksEqualDecorator) {
            return instantiateCustom(entityType, queryEngine, metaData, left, (LinksEqualDecorator) right, directClosure);
        }
        return directClosure.instantiate();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        checkWildcard(obj);
        return obj instanceof And && super.equals(obj);
    }

    @Override
    public NodeBase getClone() {
        return new And(getLeft().getClone(), getRight().getClone());
    }

    @Override
    public String getSimpleName() {
        return "and";
    }

    private static Iterable<Entity> instantiateCustom(@NotNull final String entityType,
                                                      @NotNull final QueryEngine queryEngine,
                                                      @NotNull final ModelMetaData metaData,
                                                      @NotNull final NodeBase self,
                                                      @NotNull final LinksEqualDecorator decorator,
                                                      @NotNull final Instantiatable directClosure) {
        final Iterable<Entity> selfInstance = self.instantiate(entityType, queryEngine, metaData);
        if (selfInstance instanceof EntityIterableBase) {
            final EntityIterable result = ((EntityIterableBase) selfInstance).findLinks(
                    ((EntityIterableBase) decorator.instantiateDecorated(decorator.getLinkEntityType(), queryEngine, metaData)).getSource(),
                    decorator.getLinkName());
            if (traceFindLinks) {
                final Iterator<Entity> directIt = directClosure.instantiate().iterator();
                final EntityIterator it = result.iterator();
                while (true) {
                    final boolean directHasNext = directIt.hasNext();
                    final boolean hasNext = it.hasNext();
                    if (directHasNext != hasNext) {
                        throw new EntityStoreException("Invalid custom findLinks() result: different sizes");
                    }
                    if (!hasNext) {
                        break;
                    }
                    if (!directIt.next().getId().equals(it.nextId())) {
                        throw new EntityStoreException("Invalid custom findLinks() result");
                    }
                }
            }
            return result;
        }
        return directClosure.instantiate();
    }

    private interface Instantiatable {

        Iterable<Entity> instantiate();
    }
}
