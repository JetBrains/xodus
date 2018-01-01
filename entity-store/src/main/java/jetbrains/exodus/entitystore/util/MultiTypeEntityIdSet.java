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

import jetbrains.exodus.core.dataStructures.hash.*;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.PersistentEntityId;
import jetbrains.exodus.entitystore.iterate.EntityIdSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.Map;

class MultiTypeEntityIdSet implements EntityIdSet {
    @NotNull
    private final IntHashMap<LongSet> set = new IntHashMap<>();
    private boolean holdsNull;

    MultiTypeEntityIdSet(int typeId0, @NotNull LongSet singleTypeLocalIds0,
                         int typeId1, @NotNull LongSet singleTypeLocalIds1, boolean holdsNull) {
        this.holdsNull = holdsNull;
        set.put(typeId0, singleTypeLocalIds0);
        set.put(typeId1, singleTypeLocalIds1);
    }

    public EntityIdSet add(@Nullable final EntityId id) {
        if (id == null) {
            holdsNull = true;
            return this;
        }
        final int typeId = id.getTypeId();
        final long localId = id.getLocalId();
        return add(typeId, localId);
    }

    public EntityIdSet add(final int typeId, final long localId) {
        LongSet localIds = set.get(typeId);
        if (localIds == null) {
            localIds = new PackedLongHashSet();
            set.put(typeId, localIds);
        }
        localIds.add(localId);
        return this;
    }

    public boolean contains(@Nullable final EntityId id) {
        if (id == null) {
            return holdsNull;
        }
        return contains(id.getTypeId(), id.getLocalId());
    }

    public boolean contains(final int typeId, final long localId) {
        LongSet localIds = set.get(typeId);
        return localIds != null && localIds.contains(localId);
    }

    public boolean remove(@Nullable final EntityId id) {
        if (id == null) {
            final boolean result = holdsNull;
            holdsNull = false;
            return result;
        }
        return remove(id.getTypeId(), id.getLocalId());
    }

    public boolean remove(final int typeId, final long localId) {
        LongSet localIds = set.get(typeId);
        return localIds != null && localIds.remove(localId);
    }

    public int count() {
        return -1;
    }

    @Override
    @NotNull
    public LongSet getTypeSetSnapshot(int typeId) {
        final LongSet typeSet = set.get(typeId);
        if (typeSet != null) {
            return new LongHashSet(typeSet);
        }
        return LongSet.EMPTY;
    }

    @Override
    public Iterator<EntityId> iterator() {
        final Iterator<Map.Entry<Integer, LongSet>> entries = set.entrySet().iterator();
        return new Iterator<EntityId>() {

            private int typeId = -1;
            @NotNull
            private LongIterator it = LongIterator.EMPTY;
            private boolean hasNull = holdsNull;

            @Override
            public boolean hasNext() {
                while (!it.hasNext()) {
                    if (!entries.hasNext()) {
                        return hasNull;
                    }
                    final Map.Entry<Integer, LongSet> nextTypeSet = entries.next();
                    typeId = nextTypeSet.getKey();
                    it = nextTypeSet.getValue().iterator();
                }
                return true;
            }

            @Override
            public EntityId next() {
                if (!it.hasNext()) {
                    if (!hasNull) {
                        throw new IllegalStateException();
                    }
                    hasNull = false;
                    return null;
                }
                return new PersistentEntityId(typeId, it.next());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
