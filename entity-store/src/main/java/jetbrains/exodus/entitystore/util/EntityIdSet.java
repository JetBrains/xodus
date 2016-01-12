/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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

import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.core.dataStructures.hash.LongHashSet;
import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import jetbrains.exodus.core.dataStructures.hash.LongSet;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.PersistentEntityId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.Map;

public class EntityIdSet implements Iterable<EntityId> {

    public static final EntityIdSet EMPTY_SET = new EntityIdSet();

    private final IntHashMap<LongSet> set;
    private int singleTypeId;
    private LongSet singleTypeLocalIds;
    private boolean holdsNull;

    public EntityIdSet() {
        set = new IntHashMap<>();
    }

    public void add(@Nullable final EntityId id) {
        if (id == null) {
            holdsNull = true;
            return;
        }
        final int typeId = id.getTypeId();
        final long localId = id.getLocalId();
        add(typeId, localId);
    }

    public void add(final int typeId, final long localId) {
        LongSet localIds = singleTypeLocalIds;
        if (localIds != null) {
            if (typeId != singleTypeId) {
                localIds = null;
            }
        } else {
            localIds = set.get(typeId);
        }
        if (localIds == null) {
            localIds = new LongHashSet(100);
            set.put(typeId, localIds);
            if (set.size() > 1) {
                singleTypeLocalIds = null;
            } else {
                singleTypeId = typeId;
                singleTypeLocalIds = localIds;
            }
        }
        localIds.add(localId);
    }

    public boolean contains(@Nullable final EntityId id) {
        if (id == null) {
            return holdsNull;
        }
        return contains(id.getTypeId(), id.getLocalId());
    }

    public boolean contains(final int typeId, final long localId) {
        LongSet localIds = singleTypeLocalIds;
        if (localIds == null) {
            localIds = set.get(typeId);
            return localIds != null && localIds.contains(localId);
        }
        return singleTypeId == typeId && localIds.contains(localId);
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
        LongSet localIds = singleTypeLocalIds;
        if (localIds == null) {
            localIds = set.get(typeId);
            return localIds != null && localIds.remove(localId);
        }
        return singleTypeId == typeId && localIds.remove(localId);
    }

    @Nullable
    public LongSet getTypeSet(int typeId) {
        return set.get(typeId);
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
