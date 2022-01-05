/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.iterate.cached.iterator;

import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.entitystore.iterate.NonDisposableEntityIterator;
import jetbrains.exodus.entitystore.iterate.OrderedEntityIdCollection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

public class OrderedEntityIdCollectionIterator extends NonDisposableEntityIterator {
    private int index = 0;
    @NotNull
    private final OrderedEntityIdCollection source;
    @NotNull
    private final Iterator<EntityId> sourceIterator;

    public OrderedEntityIdCollectionIterator(@NotNull EntityIterableBase iterable, @NotNull OrderedEntityIdCollection source) {
        super(iterable);
        this.source = source;
        this.sourceIterator = source.iterator();
    }

    @Override
    public boolean skip(int number) {
        while (number-- > 0 && sourceIterator.hasNext()) {
            nextIdImpl();
        }
        return sourceIterator.hasNext();
    }

    @Override
    @Nullable
    public EntityId nextId() {
        EntityId result = sourceIterator.next();
        index++;
        return result;
    }

    @Override
    @Nullable
    public EntityId getLast() {
        return source.getLast();
    }

    @Override
    @Nullable
    public EntityId nextIdImpl() {
        EntityId result = sourceIterator.next();
        index++;
        return result;
    }

    @Override
    protected boolean hasNextImpl() {
        return sourceIterator.hasNext();
    }

    @Override
    protected int getIndex() {
        return index;
    }
}
