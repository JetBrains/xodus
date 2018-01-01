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
package jetbrains.exodus.entitystore.iterate;

import jetbrains.exodus.core.dataStructures.IntArrayList;
import jetbrains.exodus.core.dataStructures.LongArrayList;
import jetbrains.exodus.entitystore.EntityId;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.entitystore.iterate.cached.*;
import jetbrains.exodus.entitystore.util.ImmutableSingleTypeEntityIdBitSet;
import jetbrains.exodus.entitystore.util.ImmutableSingleTypeEntityIdCollection;
import jetbrains.exodus.entitystore.util.IntArrayListSpinAllocator;
import jetbrains.exodus.entitystore.util.LongArrayListSpinAllocator;
import org.jetbrains.annotations.NotNull;

import static jetbrains.exodus.entitystore.iterate.EntityIterableBase.NULL_TYPE_ID;

public class EntityIdArrayCachedInstanceIterableFactory {
    public static final int MAX_COMPRESSED_SET_LOAD_FACTOR = 64;

    public static CachedInstanceIterable createInstance(@NotNull final PersistentStoreTransaction txn,
                                                        @NotNull final EntityIterableBase source) {
        return createInstance(txn, source, (EntityIteratorBase) source.getIteratorImpl(txn));
    }

    public static CachedInstanceIterable createInstance(@NotNull final PersistentStoreTransaction txn,
                                                        @NotNull final EntityIterableBase source,
                                                        @NotNull final EntityIteratorBase it) {
        try {
            if (!it.hasNext()) {
                return new EmptyCachedInstanceIterable(txn, source);
            } else {
                final IntArrayList typeIds = IntArrayListSpinAllocator.alloc();
                final LongArrayList localIds = LongArrayListSpinAllocator.alloc();
                long min;
                long max;
                try {
                    boolean onlyOneTypeId = true;
                    boolean localSorted = true;
                    if (source.isSortedById()) {
                        int lastTypeId = -1;
                        EntityId id = it.nextId();
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
                                    onlyOneTypeId = false;
                                    typeIds.add(localIds.size() - 1); // add upper boundary for previous
                                }
                                typeIds.add(nextTypeId);
                                lastTypeId = nextTypeId;
                            }
                            if (!it.hasNext()) {
                                if (!onlyOneTypeId) {
                                    typeIds.add(localIds.size()); // add boundary for last
                                }
                                break;
                            }
                            id = it.nextId();
                        }
                        min = localIds.get(0);
                        max = localIds.get(localIds.size() - 1);
                    } else {
                        int lastTypeId = -1;
                        long lastLocalId = -1;
                        min = Long.MAX_VALUE;
                        max = Long.MIN_VALUE;
                        boolean compact = true;
                        EntityId id = it.nextId();
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
                                            onlyOneTypeId = false;
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
                            if (nextLocalId > max) {
                                max = nextLocalId;
                            }
                            if (nextLocalId < min) {
                                min = nextLocalId;
                            }
                            if (compact) {
                                if (localSorted) {
                                    if (nextTypeId > lastTypeId) {
                                        if (lastTypeId != -1) {
                                            onlyOneTypeId = false;
                                            typeIds.add(localIds.size() - 1); // add upper boundary for previous
                                        }
                                        typeIds.add(nextTypeId);
                                    }
                                    lastTypeId = nextTypeId;
                                } else {
                                    if (typeIds.size() > 1 || nextTypeId != lastTypeId) {
                                        onlyOneTypeId = false;
                                        compact = false;
                                        addNextTypeId(nextTypeId, typeIds, localIds);
                                    }
                                }
                            } else {
                                typeIds.add(nextTypeId);
                            }
                            if (!it.hasNext()) {
                                if (compact && !onlyOneTypeId) {
                                    typeIds.add(localIds.size()); // add boundary for last
                                }
                                break;
                            }
                            id = it.nextId();
                        }
                    }
                    if (localSorted) {
                        if (onlyOneTypeId) {
                            return makeSingleTypeSortedIterable(txn, source, it, typeIds, localIds, min, max);
                        } else {
                            return new MultiTypeSortedEntityIdArrayCachedInstanceIterable(
                                    txn, source, typeIds.toArray(), localIds.toArray(), it.toSet()
                            );
                        }
                    } else {
                        if (onlyOneTypeId) {
                            return makeSingleTypeUnsortedIterable(txn, source, it, typeIds, localIds, min, max);
                        } else {
                            return new MultiTypeUnsortedEntityIdArrayCachedInstanceIterable(
                                    txn, source, typeIds.toArray(), localIds.toArray(), it.toSet()
                            );
                        }
                    }
                } finally {
                    LongArrayListSpinAllocator.dispose(localIds);
                    IntArrayListSpinAllocator.dispose(typeIds);
                }
            }
        } finally {
            it.disposeIfShouldBe();
        }
    }

    @NotNull
    private static CachedInstanceIterable makeSingleTypeSortedIterable(
            @NotNull PersistentStoreTransaction txn, @NotNull EntityIterableBase source, @NotNull EntityIteratorBase it,
            IntArrayList typeIds, LongArrayList localIds, long min, long max
    ) {
        final int typeId = typeIds.get(0);
        if (typeId != NULL_TYPE_ID) {
            final int length = localIds.size();
            if (length > 1) {
                if (min >= 0) {
                    final long range = max - min + 1;
                    if (range < Integer.MAX_VALUE
                            && range <= ((long) MAX_COMPRESSED_SET_LOAD_FACTOR * length)) {
                        final SortedEntityIdSet set = new ImmutableSingleTypeEntityIdBitSet(
                                typeId, localIds.getInstantArray(), length
                        );
                        return new SingleTypeSortedSetEntityIdCachedInstanceIterable(txn, source, typeId, set);
                    }
                }
            }
        }
        return new SingleTypeSortedEntityIdArrayCachedInstanceIterable(txn, source, typeId, localIds.toArray(), it.toSet());
    }

    @NotNull
    private static CachedInstanceIterable makeSingleTypeUnsortedIterable(
            @NotNull PersistentStoreTransaction txn, @NotNull EntityIterableBase source, @NotNull EntityIteratorBase it,
            IntArrayList typeIds, LongArrayList localIds, long min, long max
    ) {
        return new SingleTypeUnsortedEntityIdArrayCachedInstanceIterable(
                txn, source, typeIds.get(0), localIds.toArray(), it.toSet(), min, max
        );
    }

    @NotNull
    public static OrderedEntityIdCollection makeIdCollection(int typeId, long[] localIds) {
        final int length = localIds.length;
        if (length > 1) {
            final long min = localIds[0];
            if (min >= 0) {
                final long range = localIds[length - 1] - min + 1;
                if (range < Integer.MAX_VALUE
                        && range <= ((long) MAX_COMPRESSED_SET_LOAD_FACTOR * length)) {
                    return new ImmutableSingleTypeEntityIdBitSet(typeId, localIds, length);
                }
            }
        }
        return new ImmutableSingleTypeEntityIdCollection(typeId, localIds);
    }

    private static void addNextTypeId(final int nextTypeId, IntArrayList typeIds, LongArrayList localIds) {
        final int[] typeIdsCopy = typeIds.toArray();
        int length = typeIds.size();
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
