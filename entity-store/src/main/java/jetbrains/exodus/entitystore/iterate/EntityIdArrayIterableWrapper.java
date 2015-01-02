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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.core.dataStructures.IntArrayList;
import jetbrains.exodus.core.dataStructures.LongArrayList;
import jetbrains.exodus.entitystore.*;
import jetbrains.exodus.entitystore.util.EntityIdSet;
import jetbrains.exodus.entitystore.util.IntArrayListSpinAllocator;
import jetbrains.exodus.entitystore.util.LongArrayListSpinAllocator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public class EntityIdArrayIterableWrapper extends CachedWrapperIterable {

    private static final int[] EMPTY_TYPE_IDS = new int[0];
    private static final long[] EMPTY_LOCAL_IDS = new long[0];
    private static final int NULL_TYPE_ID = Integer.MIN_VALUE;

    private final boolean singleTypeId;
    @NotNull
    private final int[] typeIds;
    @NotNull
    private final long[] localIds;
    private final boolean isSortedById;
    @Nullable
    private EntityIdSet idSet;

    @SuppressWarnings({"ConstantConditions", "OverlyNestedMethod", "OverlyLongMethod", "OverriddenMethodCallDuringObjectConstruction"})
    public EntityIdArrayIterableWrapper(@NotNull final PersistentStoreTransaction txn,
                                        @NotNull final PersistentEntityStoreImpl store,
                                        @NotNull final EntityIterableBase source) {
        super(store, source);
        final EntityIteratorBase it = (EntityIteratorBase) source.getIteratorImpl(txn);
        try {
            if (!it.hasNext()) {
                singleTypeId = true;
                isSortedById = true;
                typeIds = EMPTY_TYPE_IDS;
                localIds = EMPTY_LOCAL_IDS;
            } else {
                final IntArrayList typeIds = IntArrayListSpinAllocator.alloc();
                final LongArrayList localIds = LongArrayListSpinAllocator.alloc();
                try {
                    if (source.isSortedById()) {
                        int lastTypeId = -1;
                        boolean first = true;
                        EntityId id = extractNextId(it);
                        while (true) {
                            final int nextTypeId;
                            if (id == null) {
                                nextTypeId = NULL_TYPE_ID;
                                localIds.add(0);
                            } else {
                                nextTypeId = id.getTypeId();
                                localIds.add(id.getLocalId());
                            }
                            if (nextTypeId != lastTypeId) {
                                if (lastTypeId != -1) {
                                    first = false;
                                    typeIds.add(localIds.size() - 1); // add upper boundary for previous
                                }
                                typeIds.add(nextTypeId);
                                lastTypeId = nextTypeId;
                            }
                            if (!it.hasNext()) {
                                if (!first) {
                                    typeIds.add(localIds.size()); // add boundary for last
                                }
                                break;
                            }
                            id = extractNextId(it);
                        }
                        singleTypeId = first;
                        isSortedById = true;
                        this.typeIds = typeIds.toArray();
                        this.localIds = localIds.toArray();
                    } else {
                        int lastTypeId = -1;
                        long lastLocalId = -1;
                        boolean first = true;
                        boolean localSorted = true;
                        boolean compact = true;
                        EntityId id = extractNextId(it);
                        while (true) {
                            final int nextTypeId;
                            final long nextLocalId;
                            if (id == null) {
                                nextTypeId = NULL_TYPE_ID;
                                nextLocalId = 0;
                            } else {
                                nextTypeId = id.getTypeId();
                                nextLocalId = id.getLocalId();
                            }
                            if (localSorted) {
                                if (lastTypeId > nextTypeId || lastTypeId == nextTypeId && lastLocalId > nextLocalId) {
                                    final int length;
                                    if (nextTypeId == NULL_TYPE_ID && (length = localIds.size()) <= 1) {
                                        if (length == 1) { // direct conversion
                                            first = false;
                                            localSorted = false;
                                            compact = false;
                                        } else {
                                            typeIds.add(NULL_TYPE_ID);
                                        }
                                        lastLocalId = nextLocalId;
                                    } else {
                                        localSorted = false;
                                    }
                                } else {
                                    lastLocalId = nextLocalId;
                                }
                            }
                            localIds.add(nextLocalId);
                            if (compact) {
                                if (localSorted) {
                                    if (nextTypeId > lastTypeId) {
                                        if (lastTypeId != -1) {
                                            first = false;
                                            typeIds.add(localIds.size() - 1); // add upper boundary for previous
                                        }
                                        typeIds.add(nextTypeId);
                                    }
                                    lastTypeId = nextTypeId;
                                } else {
                                    final int length = typeIds.size();
                                    if (length > 1 || nextTypeId != lastTypeId) {
                                        first = false;
                                        compact = false;
                                        final int[] typeIdsCopy = typeIds.toArray();
                                        final int maxBound = localIds.size() - 1;
                                        typeIds.ensureCapacity(maxBound);
                                        typeIds.clear();
                                        int i = 0;
                                        int j = 0;
                                        int currentBound = 0;
                                        int typeId = 0;
                                        while (j < maxBound) {
                                            ++j;
                                            if (j > currentBound) {
                                                typeId = typeIdsCopy[i];
                                                ++i;
                                                currentBound = i < length ? typeIdsCopy[i] : maxBound;
                                                ++i;
                                            }
                                            typeIds.add(typeId);
                                        }
                                        typeIds.add(nextTypeId);
                                    }
                                }
                            } else {
                                typeIds.add(nextTypeId);
                            }
                            if (!it.hasNext()) {
                                if (compact && !first) {
                                    typeIds.add(localIds.size()); // add boundary for last
                                }
                                break;
                            }
                            id = extractNextId(it);
                        }
                        singleTypeId = first;
                        isSortedById = localSorted;
                        this.typeIds = typeIds.toArray();
                        this.localIds = localIds.toArray();
                    }
                } finally {
                    LongArrayListSpinAllocator.dispose(localIds);
                    IntArrayListSpinAllocator.dispose(typeIds);
                }
            }
        } finally {
            it.disposeIfShouldBe();
        }
        idSet = null;
    }

    protected EntityId extractNextId(final EntityIterator it) {
        return it.nextId();
    }

    @Override
    public boolean isSortedById() {
        return isSortedById;
    }

    @SuppressWarnings({"AssignmentToForLoopParameter"})
    @Override
    protected int indexOfImpl(@NotNull final EntityId entityId) {
        final long localId = entityId.getLocalId();
        if (isSortedById) {
            if (singleTypeId) {
                final int result = Arrays.binarySearch(localIds, localId);
                if (result >= 0 && typeIds[0] == entityId.getTypeId()) {
                    return result;
                }
            } else {
                final int typeId = entityId.getTypeId();
                int prevBound = 0;
                final int length = typeIds.length;
                for (int i = 0; i < length; ++i) {
                    if (typeIds[i] == typeId) {
                        ++i;
                        final int result = Arrays.binarySearch(localIds, prevBound, typeIds[i], localId);

                        if (result >= 0) {
                            return result;
                        }
                        break;
                    } else {
                        ++i;
                        prevBound = typeIds[i];
                    }
                }
            }
        } else {
            if (singleTypeId) {
                if (entityId.getTypeId() == typeIds[0]) {
                    return LongArrayList.indexOf(localIds, localId);
                }
            } else {
                int result = 0;
                do {
                    if (localIds[result] == localId && typeIds[result] == entityId.getTypeId()) {
                        return result;
                    }
                    ++result;
                } while (result < localIds.length);
            }
        }
        return -1;
    }

    @Override
    @NotNull
    public EntityIteratorBase getIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        if (localIds.length == 0) {
            return EntityIteratorBase.EMPTY;
        }
        if (singleTypeId) {
            final int typeId = typeIds[0];
            if (typeId == NULL_TYPE_ID) {
                return new EntityIdArrayIteratorNullTypeId();
            }
            return new EntityIdArrayIteratorSingleTypeId(typeId);
        } else {
            if (isSortedById) {
                return new EntityIdArrayIteratorPacked();
            }
            return new EntityIdArrayIteratorUnpacked();
        }
    }

    @Override
    @NotNull
    public EntityIteratorBase getReverseIteratorImpl(@NotNull final PersistentStoreTransaction txn) {
        if (localIds.length == 0) {
            return EntityIteratorBase.EMPTY;
        }
        if (singleTypeId) {
            final int typeId = typeIds[0];
            if (typeId == NULL_TYPE_ID) {
                return new ReverseEntityIdArrayIteratorNullTypeId();
            }
            return new ReverseEntityIdArrayIteratorSingleTypeId(typeId);
        } else {
            if (isSortedById) {
                return new ReverseEntityIdArrayIteratorPacked();
            }
            return new ReverseEntityIdArrayIteratorUnpacked();
        }
    }

    @Override
    protected long countImpl(@NotNull final PersistentStoreTransaction txn) {
        return localIds.length;
    }

    @Override
    @SuppressWarnings({"OverlyLongMethod", "OverlyNestedMethod", "AssignmentToForLoopParameter"})
    public EntityIdSet toSet(@NotNull final PersistentStoreTransaction txn) {
        if (idSet == null) {
            final int count = typeIds.length;
            if (count == 0) {
                return EntityIdSet.EMPTY_SET;
            }
            final EntityIdSet result;
            if (count == 1) {
                result = new EntityIdSet(); //TODO: replace with some kind of "nano" single element set
                final int typeId = typeIds[0];
                if (typeId == NULL_TYPE_ID) {
                    result.add(null);
                } else {
                    for (long localId : localIds) {
                        result.add(typeId, localId);
                    }
                }
            } else {
                result = new EntityIdSet();
                if (isSortedById) {
                    int j = 0;
                    for (int i = 0; i < count; ++i) {
                        final int typeId = typeIds[i];
                        ++i;
                        final int upperBound = typeIds[i];
                        if (typeId == NULL_TYPE_ID) {
                            while (j < upperBound) {
                                result.add(null);
                                ++j;
                            }
                        } else {
                            while (j < upperBound) {
                                result.add(typeId, localIds[j++]);
                            }
                        }
                    }
                } else {
                    for (int i = 0; i < count; ++i) {
                        final int typeId = typeIds[i];
                        if (typeId == NULL_TYPE_ID) {
                            result.add(null);
                        } else {
                            result.add(typeId, localIds[i]);
                        }
                    }
                }
            }
            idSet = result;
        }
        return idSet;
    }

    private class EntityIdArrayIteratorNullTypeId extends NonDisposableEntityIterator {

        private int index;

        private EntityIdArrayIteratorNullTypeId() {
            super(EntityIdArrayIterableWrapper.this);
            index = 0;
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
            return index < localIds.length;
        }

        @Override
        protected int getIndex() {
            return index;
        }
    }

    private class EntityIdArrayIteratorSingleTypeId extends NonDisposableEntityIterator {

        private int index;
        private final int typeId;

        private EntityIdArrayIteratorSingleTypeId(final int typeId) {
            super(EntityIdArrayIterableWrapper.this);
            index = 0;
            this.typeId = typeId;
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

    private class EntityIdArrayIteratorUnpacked extends NonDisposableEntityIterator {

        private int index;

        private EntityIdArrayIteratorUnpacked() {
            super(EntityIdArrayIterableWrapper.this);
            index = 0;
        }

        @Override
        public boolean skip(int number) {
            index += number;
            return hasNextImpl();
        }

        @Override
        @Nullable
        public EntityId nextId() {
            // for better performance of cached iterables, this method copies the nextIdImpl()
            // without try-catch block since it actually throws nothing
            final int index = this.index++;
            final int typeId = typeIds[index];
            return typeId == NULL_TYPE_ID ? null : new PersistentEntityId(typeId, localIds[index]);
        }

        @Override
        @Nullable
        public EntityId getLast() {
            final int typeId;
            final int count = localIds.length;
            if (count == 0 || (typeId = typeIds[typeIds.length - 1]) == NULL_TYPE_ID) {
                return null;
            }
            return new PersistentEntityId(typeId, localIds[count - 1]);
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            final int index = this.index++;
            final int typeId = typeIds[index];
            return typeId == NULL_TYPE_ID ? null : new PersistentEntityId(typeId, localIds[index]);
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

    private class EntityIdArrayIteratorPacked extends NonDisposableEntityIterator {

        private int index;
        private int typeId;
        private int typeIndex;
        private int currentBound;

        private EntityIdArrayIteratorPacked() {
            super(EntityIdArrayIterableWrapper.this);
            index = 0;
            typeIndex = 0;
            currentBound = 0;
        }

        @Override
        public boolean skip(int number) {
            final int index = this.index + number;
            this.index = index;
            if (hasNextImpl()) {
                while (index > currentBound) {
                    typeId = typeIds[typeIndex];
                    ++typeIndex;
                    currentBound = typeIds[typeIndex];
                    ++typeIndex;
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
            final int index = this.index++;
            if (index >= currentBound) {
                typeId = typeIds[typeIndex];
                ++typeIndex;
                currentBound = typeIds[typeIndex];
                ++typeIndex;
            }
            return typeId == NULL_TYPE_ID ? null : new PersistentEntityId(typeId, localIds[index]);
        }

        @Override
        @Nullable
        public EntityId getLast() {
            final int typeId;
            final int count = localIds.length;
            if (count == 0 || (typeId = typeIds[typeIds.length - 2]) == NULL_TYPE_ID) {
                return null;
            }
            return new PersistentEntityId(typeId, localIds[count - 1]);
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            final int index = this.index++;
            if (index >= currentBound) {
                typeId = typeIds[typeIndex];
                ++typeIndex;
                currentBound = typeIds[typeIndex];
                ++typeIndex;
            }
            return typeId == NULL_TYPE_ID ? null : new PersistentEntityId(typeId, localIds[index]);
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

    private class ReverseEntityIdArrayIteratorNullTypeId extends NonDisposableEntityIterator {

        private int index;

        private ReverseEntityIdArrayIteratorNullTypeId() {
            super(EntityIdArrayIterableWrapper.this);
            index = localIds.length;
        }

        @Override
        public boolean skip(int number) {
            index -= number;
            return hasNextImpl();
        }

        @Override
        @Nullable
        public EntityId nextId() {
            --index;
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
            --index;
            return null;
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

    private class ReverseEntityIdArrayIteratorSingleTypeId extends NonDisposableEntityIterator {

        private int index;
        private final int typeId;

        private ReverseEntityIdArrayIteratorSingleTypeId(final int typeId) {
            super(EntityIdArrayIterableWrapper.this);
            index = localIds.length;
            this.typeId = typeId;
        }

        @Override
        public boolean skip(int number) {
            index -= number;
            return hasNextImpl();
        }

        @Override
        @Nullable
        public EntityId nextId() {
            final int index = --this.index;
            return new PersistentEntityId(typeId, localIds[index]);
        }

        @Override
        @Nullable
        public EntityId getLast() {
            return new PersistentEntityId(typeId, localIds[0]);
        }

        @Override
        @Nullable
        public EntityId nextIdImpl() {
            final int index = --this.index;
            return new PersistentEntityId(typeId, localIds[index]);
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

    private class ReverseEntityIdArrayIteratorPacked extends NonDisposableEntityIterator {

        private int index;
        private int typeId;
        private int typeIndex;
        private int currentBound;

        private ReverseEntityIdArrayIteratorPacked() {
            super(EntityIdArrayIterableWrapper.this);
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

    private class ReverseEntityIdArrayIteratorUnpacked extends NonDisposableEntityIterator {

        private int index;

        private ReverseEntityIdArrayIteratorUnpacked() {
            super(EntityIdArrayIterableWrapper.this);
            index = localIds.length;
        }

        @Override
        public boolean skip(int number) {
            index -= number;
            return hasNextImpl();
        }

        @Override
        @Nullable
        public EntityId nextId() {
            // for better performance of cached iterables, this method copies the nextIdImpl()
            // without try-catch block since it actually throws nothing
            final int index = --this.index;
            final int typeId = typeIds[index];
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
            final int typeId = typeIds[index];
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
}
