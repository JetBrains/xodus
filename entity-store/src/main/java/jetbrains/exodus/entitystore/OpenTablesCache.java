/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore;

import jetbrains.exodus.entitystore.tables.Table;
import org.jctools.maps.NonBlockingHashMapLong;
import org.jetbrains.annotations.NotNull;

class OpenTablesCache {

    private final TableCreator creator;
    private final Object lock;
    private NonBlockingHashMapLong<Table> cache;

    OpenTablesCache(@NotNull final TableCreator creator) {
        this.creator = creator;
        lock = new Object();
        cache = new NonBlockingHashMapLong<>();
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
                    var newCache = cloneCache();
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
                var newCache = cloneCache();
                newCache.remove(entityTypeId);
                cache = newCache;
            }
        }
    }

    private NonBlockingHashMapLong<Table> cloneCache() {
        final var currentCache = cache;
        final var result = new NonBlockingHashMapLong<Table>(currentCache.size());
        result.putAll(currentCache);
        return result;
    }

    interface TableCreator {

        @NotNull
        Table createTable(@NotNull final PersistentStoreTransaction txn, final int entityTypeId);
    }
}
