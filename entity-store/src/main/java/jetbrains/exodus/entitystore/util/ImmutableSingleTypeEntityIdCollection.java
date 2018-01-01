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
package jetbrains.exodus.entitystore.util;

import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.PersistentEntityId;
import jetbrains.exodus.entitystore.iterate.OrderedEntityIdCollection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

public class ImmutableSingleTypeEntityIdCollection implements OrderedEntityIdCollection {

    private final int singleTypeId;
    @NotNull
    private final long[] idArray;

    public ImmutableSingleTypeEntityIdCollection(int singleTypeId, @NotNull long[] idArray) {
        this.singleTypeId = singleTypeId;
        this.idArray = idArray;
    }

    @Override
    public int count() {
        return idArray.length;
    }

    @Override
    public EntityId getFirst() {
        return new PersistentEntityId(singleTypeId, idArray[0]);
    }

    @Override
    public EntityId getLast() {
        return new PersistentEntityId(singleTypeId, idArray[idArray.length - 1]);
    }

    @NotNull
    public long[] getIdArray() {
        return idArray;
    }

    @Override
    public Iterator<EntityId> iterator() {
        return new Iterator<EntityId>() {
            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < idArray.length;
            }

            @Nullable
            @Override
            public EntityId next() {
                return new PersistentEntityId(singleTypeId, idArray[i++]);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

    }

    @Override
    public Iterator<EntityId> reverseIterator() {
        return new Iterator<EntityId>() {
            private int i = idArray.length;

            @Override
            public boolean hasNext() {
                return i > 0;
            }

            @Nullable
            @Override
            public EntityId next() {
                return new PersistentEntityId(singleTypeId, idArray[--i]);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
