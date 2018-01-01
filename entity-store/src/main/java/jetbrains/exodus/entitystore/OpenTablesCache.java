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

import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import jetbrains.exodus.entitystore.tables.Table;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

class OpenTablesCache {

    private final TableCreator creator;
    private final Object lock;
    private IntHashMap<Table> cache;

    OpenTablesCache(@NotNull final TableCreator creator) {
        this.creator = creator;
        lock = new Object();
        cache = new IntHashMap<>();
    }

    Table get(@NotNull final PersistentStoreTransaction txn, final int entityTypeId) {
        Table result = cache.get(entityTypeId);
        if (result != null) {
            return result;
        }
        synchronized (lock) {
            result = cache.get(entityTypeId);
            if (result == null) {
                result = creator.createTable(txn, entityTypeId);
                if (result.canBeCached()) {
                    final IntHashMap<Table> newCache = cloneCache();
                    newCache.put(entityTypeId, result);
                    cache = newCache;
                }
            }
        }
        return result;
    }

    void remove(final int entityTypeId) {
        synchronized (lock) {
            if (cache.containsKey(entityTypeId)) {
                final IntHashMap<Table> newCache = cloneCache();
                newCache.remove(entityTypeId);
                cache = newCache;
            }
        }
    }

    private IntHashMap<Table> cloneCache() {
        final IntHashMap<Table> currentCache = cache;
        final IntHashMap<Table> result = new IntHashMap<>(currentCache.size());
        currentCache.forEachEntry(new ObjectProcedure<Map.Entry<Integer, Table>>() {
            @Override
            public boolean execute(Map.Entry<Integer, Table> entry) {
                result.put(entry.getKey(), entry.getValue());
                return true;
            }
        });
        return result;
    }

    interface TableCreator {

        @NotNull
        Table createTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId);
    }
}
