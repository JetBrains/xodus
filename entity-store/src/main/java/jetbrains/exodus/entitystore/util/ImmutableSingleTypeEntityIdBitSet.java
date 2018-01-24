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

import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import jetbrains.exodus.core.dataStructures.hash.LongSet;
import jetbrains.exodus.core.dataStructures.hash.PackedLongHashSet;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.PersistentEntityId;
import jetbrains.exodus.entitystore.iterate.EntityIdSet;
import jetbrains.exodus.entitystore.iterate.SortedEntityIdSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ImmutableSingleTypeEntityIdBitSet implements SortedEntityIdSet {
    private final int singleTypeId;
    private final int size;
    private final long min;
    private final long max;
    private final BitSet data;

    public ImmutableSingleTypeEntityIdBitSet(final int singleTypeId, final long[] source) {
        this(singleTypeId, source, source.length);
    }

    public ImmutableSingleTypeEntityIdBitSet(final int singleTypeId, final long[] source, int length) {
        if (length > source.length) {
            throw new IllegalArgumentException();
        }
        this.singleTypeId = singleTypeId;
        min = source[0];
        max = source[length - 1];
        final long bitsCount = max - min + 1;
        if (min < 0 || bitsCount >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException();
        }
        data = new BitSet((int) bitsCount);
        for (int i = 0; i < length; i++) {
            data.set((int) (source[i] - min));
        }
        size = data.cardinality();
    }

    public ImmutableSingleTypeEntityIdBitSet(final int singleTypeId, long min, long max, final LongIterator source) {
        this.singleTypeId = singleTypeId;
        this.min = min;
        this.max = max;
        final long bitsCount = max - min + 1;
        if (min < 0 || bitsCount >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException();
        }
        data = new BitSet((int) bitsCount);
        while (source.hasNext()) {
            data.set((int) (source.next() - min));
        }
        size = data.cardinality();
    }

    @Override
    public EntityIdSet add(@Nullable EntityId id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public EntityIdSet add(int typeId, long localId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(@Nullable EntityId id) {
        return id != null && contains(id.getTypeId(), id.getLocalId());
    }

    @Override
    public boolean contains(int typeId, long localId) {
        return typeId == singleTypeId
                && localId >= min
                && localId <= max
                && data.get((int) (localId - min));
    }

    @Override
    public boolean remove(@Nullable EntityId id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(int typeId, long localId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int count() {
        return size;
    }

    @Override
    public Iterator<EntityId> iterator() {
        return new IdIterator();
    }

    @Override
    public int indexOf(@NotNull EntityId entityId) {
        long id = entityId.getLocalId();
        if (!contains(entityId.getTypeId(), id)) {
            return -1;
        }
        int nextBitIndex = data.nextSetBit(0);
        int result = 0;
        if (nextBitIndex != -1) {
            id -= min;
            while (nextBitIndex < id) {
                result++;
                nextBitIndex = data.nextSetBit(nextBitIndex + 1);
                if (nextBitIndex == -1) {
                    break;
                }
            }
        }
        return result;
    }

    @Override
    public EntityId getFirst() {
        return new PersistentEntityId(singleTypeId, min);
    }

    @Override
    public EntityId getLast() {
        return new PersistentEntityId(singleTypeId, max);
    }

    @Override
    public Iterator<EntityId> reverseIterator() {
        return new ReverseIdIterator();
    }

    @NotNull
    @Override
    public LongSet getTypeSetSnapshot(int typeId) {
        if (typeId == singleTypeId) {
            final LongSet result = new PackedLongHashSet();
            int next = data.nextSetBit(0);
            while (next != -1) {
                result.add(next + min);
                // if (next == Integer.MAX_VALUE) { break; }
                next = data.nextSetBit(next + 1);
            }
            return result;
        }
        return LongSet.EMPTY;
    }

    class IdIterator implements Iterator<EntityId> {
        int nextBitIndex = data.nextSetBit(0);

        @Override
        public boolean hasNext() {
            return nextBitIndex != -1;
        }

        @Override
        public EntityId next() {
            if (nextBitIndex != -1) {
                final int bitIndex = nextBitIndex;
                nextBitIndex = data.nextSetBit(nextBitIndex + 1);
                return new PersistentEntityId(singleTypeId, bitIndex + min);
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    class ReverseIdIterator implements Iterator<EntityId> {
        int nextBitIndex = data.previousSetBit((int) (max - min));

        @Override
        public boolean hasNext() {
            return nextBitIndex != -1;
        }

        @Override
        public EntityId next() {
            if (nextBitIndex != -1) {
                final int bitIndex = nextBitIndex;
                nextBitIndex = data.previousSetBit(nextBitIndex - 1);
                return new PersistentEntityId(singleTypeId, bitIndex + min);
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
