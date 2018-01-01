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
import jetbrains.exodus.entitystore.PersistentEntityId;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import jetbrains.exodus.entitystore.iterate.NonDisposableEntityIterator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static jetbrains.exodus.entitystore.iterate.EntityIterableBase.NULL_TYPE_ID;

public class ReverseEntityIdArrayIteratorMultiTypeIdPacked extends NonDisposableEntityIterator {
    private final int[] typeIds;
    private final long[] localIds;

    private int index;
    private int typeId = -1;
    private int typeIndex;
    private int currentBound;

    public ReverseEntityIdArrayIteratorMultiTypeIdPacked(@NotNull EntityIterableBase iterable, int[] typeIds, long[] localIds) {
        super(iterable);
        this.typeIds = typeIds;
        this.localIds = localIds;
        index = localIds.length;
        typeIndex = typeIds.length - 1;
        currentBound = localIds.length; // typeIds[typeIndex]
    }

    @Override
    public boolean skip(int number) {
        final int index = this.index - number;
        this.index = index;
        if (hasNextImpl()) {
            while (index <= currentBound) {
                --typeIndex;
                typeId = typeIds[typeIndex];
                if (typeIndex > 0) {
                    --typeIndex;
                    currentBound = typeIds[typeIndex];
                } else {
                    currentBound = 0;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    @Nullable
    public EntityId nextId() {
        // for better performance of cached iterables, this method copies the nextIdImpl()
        // without try-catch block since it actually throws nothing
        final int index = --this.index;
        if (index < currentBound) {
            --typeIndex;
            typeId = typeIds[typeIndex];
            if (typeIndex > 0) {
                --typeIndex;
                currentBound = typeIds[typeIndex];
            } else {
                currentBound = 0;
            }
        }
        return typeId == NULL_TYPE_ID ? null : new PersistentEntityId(typeId, localIds[index]);
    }

    @Override
    @Nullable
    public EntityId getLast() {
        final int typeId;
        if (localIds.length == 0 || (typeId = typeIds[0]) == NULL_TYPE_ID) {
            return null;
        }
        return new PersistentEntityId(typeId, localIds[0]);
    }

    @Override
    @Nullable
    public EntityId nextIdImpl() {
        final int index = --this.index;
        if (index < currentBound) {
            --typeIndex;
            typeId = typeIds[typeIndex];
            if (typeIndex > 0) {
                --typeIndex;
                currentBound = typeIds[typeIndex];
            } else {
                currentBound = 0;
            }
        }
        return typeId == NULL_TYPE_ID ? null : new PersistentEntityId(typeId, localIds[index]);
    }

    @Override
    protected boolean hasNextImpl() {
        return index > 0;
    }

    @Override
    protected int getIndex() {
        return index;
    }
}
