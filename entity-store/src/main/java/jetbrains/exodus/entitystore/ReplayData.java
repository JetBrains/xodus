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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.core.dataStructures.hash.HashMap;
import jetbrains.exodus.core.dataStructures.hash.HashSet;
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import jetbrains.exodus.core.dataStructures.persistent.PersistentObjectCache;
import jetbrains.exodus.entitystore.iterate.UpdatableCachedWrapperIterable;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class ReplayData {

    private static final List<EntityIterableHandle> NO_UPDATES = Collections.emptyList();

    private EntityIterableCacheAdapter cacheSnapshot;

    private final Map<PersistentStoreTransaction.HandleChecker, List<EntityIterableHandle>> changes = new HashMap<>();
    private Set<EntityIterableHandle> delete = new HashSet<>();
    private Set<EntityIterableHandle> suspicious;

    public ReplayData() {
    }

    public void updateMutableCache(@NotNull final EntityIterableCacheAdapter mutableCache,
                                   @NotNull final List<UpdatableCachedWrapperIterable> mutatedInTxn,
                                   @NotNull final PersistentStoreTransaction.HandleChecker checker) {
        final boolean alreadySeen = changes.containsKey(checker);
        if (suspicious != null && alreadySeen) {
            if (!suspicious.isEmpty()) {
                for (final EntityIterableHandle handle : suspicious) {
                    check(handle, checker, mutableCache, mutatedInTxn);
                }
            }
            final List<EntityIterableHandle> l = changes.get(checker);
            if (l != null) {
                for (final EntityIterableHandle handle : l) {
                    UpdatableCachedWrapperIterable it = (UpdatableCachedWrapperIterable) mutableCache.getObject(handle);
                    if (it != null) {
                        if (!it.isMutated()) {
                            it = it.beginUpdate();
                            // cache new mutated iterable wrapper
                            mutableCache.cacheObject(handle, it);
                            mutatedInTxn.add(it);
                        }
                        checker.update(handle, it);
                    }
                }
            }
            return;
        }
        if (!alreadySeen) {
            changes.put(checker, NO_UPDATES);
        }
        mutableCache.forEachKey(new ObjectProcedure<EntityIterableHandle>() {
            @Override
            public boolean execute(EntityIterableHandle object) {
                check(object, checker, mutableCache, mutatedInTxn);
                return true;
            }
        });
    }

    private void check(@NotNull final EntityIterableHandle handle,
                       @NotNull final PersistentStoreTransaction.HandleChecker checker,
                       @NotNull final EntityIterableCacheAdapter mutableCache,
                       @NotNull final List<UpdatableCachedWrapperIterable> mutatedInTxn) {
        switch (checker.checkHandle(handle, mutableCache)) {
            case KEEP:
                break; // do nothing, keep handle
            case REMOVE:
                delete.add(handle);
                mutableCache.remove(handle);
                break;
            case UPDATE:
                UpdatableCachedWrapperIterable it = (UpdatableCachedWrapperIterable) mutableCache.getObject(handle);
                if (it != null) {
                    if (!it.isMutated()) {
                        it = it.beginUpdate();
                        // cache new mutated iterable wrapper
                        mutableCache.cacheObject(handle, it);
                        mutatedInTxn.add(it);
                    }
                    checker.update(handle, it);
                    List<EntityIterableHandle> l = changes.get(checker);
                    if (l == NO_UPDATES) {
                        l = new ArrayList<>(8);
                        changes.put(checker, l);
                    }
                    l.add(handle);
                }
        }
    }

    public void setCacheSnapshot(@NotNull final EntityIterableCacheAdapter cache) {
        cacheSnapshot = cache;
    }

    public boolean hasCacheSnapshot() {
        return cacheSnapshot != null;
    }

    public void init(@NotNull final PersistentObjectCache<EntityIterableHandle, EntityIterableCacheAdapter.CacheItem> localCache) {
        suspicious = new HashSet<>();
        if (hasCacheSnapshot()) {
            final PersistentObjectCache<EntityIterableHandle, EntityIterableCacheAdapter.CacheItem> old = cacheSnapshot.getCacheInstance();
            localCache.forEachKey(new ObjectProcedure<EntityIterableHandle>() {
                @Override
                public boolean execute(EntityIterableHandle object) {
                    EntityIterableCacheAdapter.CacheItem oldWrapper = old.getObject(object);
                    if (oldWrapper == null || oldWrapper != localCache.getObject(object)) {
                        suspicious.add(object);
                    }
                    return true;
                }
            });
        }
    }

    public void apply(@NotNull final EntityIterableCacheAdapter localCache) {
        for (final EntityIterableHandle handle : delete) {
            localCache.remove(handle);
        }
    }
}
