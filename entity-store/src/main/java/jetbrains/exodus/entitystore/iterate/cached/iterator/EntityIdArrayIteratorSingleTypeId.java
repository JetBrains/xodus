/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore.iterate.cached.iterator;

import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.PersistentEntityId;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.entitystore.iterate.NonDisposableEntityIterator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class EntityIdArrayIteratorSingleTypeId extends NonDisposableEntityIterator {
    private int index = 0;
    private final int typeId;
    private final long[] localIds;

    public EntityIdArrayIteratorSingleTypeId(@NotNull EntityIterableBase iterable, int typeId, long[] localIds) {
        super(iterable);
        this.typeId = typeId;
        this.localIds = localIds;
    }

    @Override
    public boolean skip(int number) {
        index += number;
        return hasNextImpl();
    }

    @Override
    @Nullable
    public EntityId nextId() {
        final int index = this.index++;
        return new PersistentEntityId(typeId, localIds[index]);
    }

    @Override
    @Nullable
    public EntityId getLast() {
        return new PersistentEntityId(typeId, localIds[localIds.length - 1]);
    }

    @Override
    @Nullable
    public EntityId nextIdImpl() {
        final int index = this.index++;
        return new PersistentEntityId(typeId, localIds[index]);
    }

    @Override
    protected boolean hasNextImpl() {
        return index < localIds.length;
    }

    @Override
    protected int getIndex() {
        return index;
    }
}
