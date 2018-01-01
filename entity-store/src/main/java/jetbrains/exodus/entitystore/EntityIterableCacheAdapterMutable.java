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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.core.dataStructures.hash.*;
import jetbrains.exodus.core.dataStructures.persistent.EvictListener;
import jetbrains.exodus.entitystore.iterate.CachedInstanceIterable;
import jetbrains.exodus.entitystore.iterate.EntityIterableBase;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class EntityIterableCacheAdapterMutable extends EntityIterableCacheAdapter {

    private final HandlesDistribution handlesDistribution;

    private EntityIterableCacheAdapterMutable(@NotNull final PersistentEntityStoreConfig config,
                                              @NotNull final HandlesDistribution handlesDistribution,
                                              @NotNull HashMap<EntityIterableHandle, Updatable> stickyObjects) {
        super(config, handlesDistribution.cache, stickyObjects);
        this.handlesDistribution = handlesDistribution;
    }

    @NotNull
    EntityIterableCacheAdapter endWrite() {
        return new EntityIterableCacheAdapter(config, cache.endWrite(), stickyObjects);
    }

    void update(@NotNull final PersistentStoreTransaction.HandleCheckerAdapter checker) {
        final ObjectProcedure<EntityIterableHandle> procedure = new ObjectProcedure<EntityIterableHandle>() {
            @Override
            public boolean execute(EntityIterableHandle object) {
                if (checker.checkHandle(object)) {
                    remove(object);
                }
                return true;
            }
        };

        final int linkId;
        final int propertyId;
        final int typeId;
        final int typeIdAffectingCreation;

        if ((linkId = checker.getLinkId()) >= 0) {
            handlesDistribution.byLink.forEachHandle(linkId, procedure);
        } else if ((propertyId = checker.getPropertyId()) >= 0) {
            handlesDistribution.byProp.forEachHandle(propertyId, procedure);
        } else if ((typeIdAffectingCreation = checker.getTypeIdAffectingCreation()) >= 0) {
            handlesDistribution.byTypeIdAffectingCreation.forEachHandle(typeIdAffectingCreation, procedure);
        } else if ((typeId = checker.getTypeId()) >= 0) {
            handlesDistribution.byTypeId.forEachHandle(typeId, procedure);
            handlesDistribution.byTypeId.forEachHandle(EntityIterableBase.NULL_TYPE_ID, procedure);
        } else {
            forEachKey(procedure);
        }

        for (final EntityIterableHandle handle : stickyObjects.keySet()) {
            checker.checkHandle(handle);
        }
    }

    @Override
    void cacheObject(@NotNull EntityIterableHandle key, @NotNull CachedInstanceIterable it) {
        super.cacheObject(key, it);
        handlesDistribution.addHandle(key);
    }

    void registerStickyObject(@NotNull final EntityIterableHandle handle, Updatable object) {
        stickyObjects.put(handle, object);
    }

    @Override
    void remove(@NotNull EntityIterableHandle key) {
        if (key.isSticky()) {
            throw new IllegalStateException("Cannot remove sticky object");
        }
        super.remove(key);
        handlesDistribution.removeHandle(key);
    }

    @Override
    void clear() {
        super.clear();
        // stickyObjects.clear();
        handlesDistribution.clear();
    }

    void cacheObjectNotAffectingHandleDistribution(@NotNull EntityIterableHandle handle, CachedInstanceIterable it) {
        super.cacheObject(handle, it);
    }

    static EntityIterableCacheAdapterMutable create(@NotNull final EntityIterableCacheAdapter source) {
        HandlesDistribution handlesDistribution = new HandlesDistribution(source.cache);
        return new EntityIterableCacheAdapterMutable(source.config, handlesDistribution, new HashMap<>(source.stickyObjects));
    }

    private static class HandlesDistribution implements EvictListener<EntityIterableHandle, CacheItem> {

        private final NonAdjustablePersistentObjectCache<EntityIterableHandle, CacheItem> cache;
        private final Set<EntityIterableHandle> removed;
        private final FieldIdGroupedHandles byLink;
        private final FieldIdGroupedHandles byProp;
        private final FieldIdGroupedHandles byTypeId;
        private final FieldIdGroupedHandles byTypeIdAffectingCreation;

        HandlesDistribution(@NotNull final NonAdjustablePersistentObjectCache<EntityIterableHandle, CacheItem> cache) {
            this.cache = cache.getClone(this);
            int count = cache.count();
            // this set is intentionally created quite disperse in order to reduce number of calls
            // to EntityIterableHandle.equals() during iteration of handle clusters
            removed = new HashSet<>(10, .33f);
            byLink = new FieldIdGroupedHandles(count / 16, removed);
            byProp = new FieldIdGroupedHandles(count / 16, removed);
            byTypeId = new FieldIdGroupedHandles(count / 16, removed);
            byTypeIdAffectingCreation = new FieldIdGroupedHandles(count / 16, removed);

            cache.forEachEntry(new PairProcedure<EntityIterableHandle, CacheItem>() {
                @Override
                public boolean execute(EntityIterableHandle handle, CacheItem value) {
                    CachedInstanceIterable iterable = getCachedValue(value);
                    if (iterable != null) {
                        addHandle(handle);
                    }
                    return true;
                }
            });
        }

        @Override
        public void onEvict(@NotNull final EntityIterableHandle key, CacheItem value) {
            removeHandle(key);
        }

        void removeHandle(@NotNull final EntityIterableHandle handle) {
            removed.add(handle);
        }

        void addHandle(@NotNull final EntityIterableHandle handle) {
            if (!removed.isEmpty()) {
                removed.remove(handle);
            }
            byLink.add(handle, handle.getLinkIds());
            byProp.add(handle, handle.getPropertyIds());
            byTypeId.add(handle, handle.getEntityTypeId());
            byTypeIdAffectingCreation.add(handle, handle.getTypeIdsAffectingCreation());
        }

        void clear() {
            removed.clear();
            byLink.clear();
            byProp.clear();
            byTypeId.clear();
            byTypeIdAffectingCreation.clear();
        }
    }

    private static class FieldIdGroupedHandles extends IntHashMap<List<EntityIterableHandle>> {

        private final Set<EntityIterableHandle> removed;

        FieldIdGroupedHandles(int capacity, @NotNull final Set<EntityIterableHandle> removed) {
            super(capacity);
            this.removed = removed;
        }

        // it is allowed to add EntityIterableBase.NULL_TYPE_ID
        void add(@NotNull final EntityIterableHandle handle, int fieldId) {
            List<EntityIterableHandle> handles = get(fieldId);
            if (handles == null) {
                handles = new ArrayList<>(4);
                put(fieldId, handles);
            }
            handles.add(handle);
        }

        void add(@NotNull final EntityIterableHandle handle, @NotNull int[] fieldIds) {
            for (int fieldId : fieldIds) {
                if (fieldId >= 0) {
                    add(handle, fieldId);
                }
            }
        }

        void forEachHandle(final int fieldId, final ObjectProcedure<EntityIterableHandle> procedure) {
            final List<EntityIterableHandle> handles = get(fieldId);
            if (handles != null) {
                if (removed.isEmpty()) {
                    for (final EntityIterableHandle handle : handles) {
                        procedure.execute(handle);
                    }
                } else {
                    for (final EntityIterableHandle handle : handles) {
                        if (!removed.contains(handle)) {
                            procedure.execute(handle);
                        }
                    }
                }
            }
        }
    }
}
