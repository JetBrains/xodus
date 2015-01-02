/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
import jetbrains.exodus.core.dataStructures.hash.LongSet;
import jetbrains.exodus.entitystore.EntityId;
import org.jetbrains.annotations.Nullable;

public class EntityIdSet {

    public static final EntityIdSet EMPTY_SET = new EntityIdSet();

    private final IntHashMap<LongSet> set = new IntHashMap<LongSet>();
    private int singleTypeId;
    private LongSet singleTypeLocalIds = null;
    private boolean holdsNull = false;

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
}
