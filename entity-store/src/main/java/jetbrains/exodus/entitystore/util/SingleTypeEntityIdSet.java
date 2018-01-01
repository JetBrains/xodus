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

import jetbrains.exodus.core.dataStructures.hash.LongHashSet;
import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import jetbrains.exodus.core.dataStructures.hash.LongSet;
import jetbrains.exodus.core.dataStructures.hash.PackedLongHashSet;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.PersistentEntityId;
import jetbrains.exodus.entitystore.iterate.EntityIdSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

class SingleTypeEntityIdSet implements EntityIdSet {

    private final int singleTypeId;
    @NotNull
    private final LongSet singleTypeLocalIds = new PackedLongHashSet();
    private boolean holdsNull;

    SingleTypeEntityIdSet(@Nullable final EntityId id) {
        if (this.holdsNull = (id == null)) {
            this.singleTypeId = -1;
        } else {
            this.singleTypeId = id.getTypeId();
            singleTypeLocalIds.add(id.getLocalId());
        }
    }

    SingleTypeEntityIdSet(int singleTypeId, long localId) {
        this.singleTypeId = singleTypeId;
        this.holdsNull = false;
        singleTypeLocalIds.add(localId);
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
        if (typeId == singleTypeId) {
            singleTypeLocalIds.add(localId);
            return this;
        } else {
            final LongSet moreLocalIds = new PackedLongHashSet();
            moreLocalIds.add(localId);
            return new MultiTypeEntityIdSet(typeId, moreLocalIds, singleTypeId, singleTypeLocalIds, holdsNull);
        }
    }

    public boolean contains(@Nullable final EntityId id) {
        if (id == null) {
            return holdsNull;
        }
        return contains(id.getTypeId(), id.getLocalId());
    }

    public boolean contains(final int typeId, final long localId) {
        return singleTypeId == typeId && singleTypeLocalIds.contains(localId);
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
        return singleTypeId == typeId && singleTypeLocalIds.remove(localId);
    }

    public int count() {
        return singleTypeLocalIds.size();
    }

    @Override
    @NotNull
    public LongSet getTypeSetSnapshot(int typeId) {
        if (typeId == singleTypeId) {
            return new LongHashSet(singleTypeLocalIds);
        }
        return LongSet.EMPTY;
    }

    @Override
    public Iterator<EntityId> iterator() {
        return new Iterator<EntityId>() {
            @NotNull
            private final LongIterator it = singleTypeLocalIds.iterator();
            private boolean hasNull = holdsNull;

            @Override
            public boolean hasNext() {
                return it.hasNext() || hasNull;
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
                return new PersistentEntityId(singleTypeId, it.next());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
