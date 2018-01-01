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
package jetbrains.exodus.entitystore.iterate.cached.iterator;

import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.entitystore.iterate.NonDisposableEntityIterator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class EntityIdArrayIteratorNullTypeId extends NonDisposableEntityIterator {
    private final int length;

    private int index;

    public EntityIdArrayIteratorNullTypeId(@NotNull EntityIterableBase iterable, final int length) {
        super(iterable);
        this.length = length;
    }

    @Override
    public boolean skip(int number) {
        index += number;
        return hasNextImpl();
    }

    @Override
    @Nullable
    public EntityId nextId() {
        ++index;
        return null;
    }

    @Override
    @Nullable
    public EntityId getLast() {
        return null;
    }

    @Override
    @Nullable
    public EntityId nextIdImpl() {
        ++index;
        return null;
    }

    @Override
    protected boolean hasNextImpl() {
        return index < length;
    }

    @Override
    protected int getIndex() {
        return index;
    }
}
