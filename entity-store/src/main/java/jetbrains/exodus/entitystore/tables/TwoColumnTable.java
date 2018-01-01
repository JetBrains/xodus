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
package jetbrains.exodus.entitystore.tables;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.env.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings({"HardCodedStringLiteral"})
public final class TwoColumnTable extends Table {

    private final Store first;  // 1st column -> 2nd column
    private final Store second; // 2nd column -> 1st column

    public TwoColumnTable(@NotNull final PersistentStoreTransaction txn,
                          @NotNull final String name,
                          @NotNull final StoreConfig config) {
        final PersistentEntityStoreImpl store = txn.getStore();
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final Environment env = store.getEnvironment();
        first = env.openStore(name, config, envTxn);
        second = env.openStore(secondColumnDatabaseName(name), config, envTxn);
        store.trackTableCreation(first, txn);
        store.trackTableCreation(second, txn);
    }

    public static String secondColumnDatabaseName(@NotNull final String name) {
        return name + "#reverse";
    }

    /**
     * Search for the first entry in the first database. Use this method for databases configured with no duplicates.
     *
     * @param txn   enclosing transaction
     * @param first first key.
     * @return null if no entry found, otherwise the value.
     */
    @Nullable
    public ByteIterable get(@NotNull final Transaction txn, @NotNull final ByteIterable first) {
        return this.first.get(txn, first);
    }

    /**
     * Search for the second entry in the second database. Use this method for databases configured with no duplicates.
     *
     * @param second second key (value for first).
     * @return null if no entry found, otherwise the value.
     */
    @Nullable
    public ByteIterable get2(@NotNull final Transaction txn, @NotNull final ByteIterable second) {
        return this.second.get(txn, second);
    }

    public boolean contains(@NotNull final Transaction txn,
                            @NotNull final ByteIterable first,
                            @NotNull final ByteIterable second) {
        try (Cursor cursor = getFirstIndexCursor(txn)) {
            return cursor.getSearchBoth(first, second);
        }
    }

    public boolean contains2(@NotNull final Transaction txn,
                             @NotNull final ByteIterable first,
                             @NotNull final ByteIterable second) {
        try (Cursor cursor = getSecondIndexCursor(txn)) {
            return cursor.getSearchBoth(second, first);
        }
    }

    public boolean put(@NotNull final Transaction txn,
                       @NotNull final ByteIterable first,
                       @NotNull final ByteIterable second) {
        final boolean result = this.first.put(txn, first, second);
        this.second.put(txn, second, first);
        return result;
    }

    public boolean delete(@NotNull final Transaction txn,
                          @NotNull final ByteIterable first,
                          @NotNull final ByteIterable second) {
        boolean success;
        try (Cursor cursor = getFirstIndexCursor(txn)) {
            success = cursor.getSearchBoth(first, second);
            if (!success) {
                return false;
            }
            success = cursor.deleteCurrent();
            checkStatus(success, "Failed to delete");
        }
        try (Cursor cursor = getSecondIndexCursor(txn)) {
            success = cursor.getSearchBoth(second, first);
            checkStatus(success, "Failed to delete: data mismatch in TwoColumnTable's stores");
            success = cursor.deleteCurrent();
            checkStatus(success, "Failed to delete");
        }
        return true;
    }

    @NotNull
    public Cursor getFirstIndexCursor(@NotNull final Transaction txn) {
        return first.openCursor(txn);
    }

    @NotNull
    public Cursor getSecondIndexCursor(@NotNull final Transaction txn) {
        return second.openCursor(txn);
    }

    public void truncateFirst(@NotNull final Transaction txn) {
        txn.getEnvironment().truncateStore(first.getName(), txn);
    }

    public long getPrimaryCount(@NotNull final Transaction txn) {
        return first.count(txn);
    }

    public long getSecondaryCount(@NotNull final Transaction txn) {
        return second.count(txn);
    }

    @Override
    public boolean canBeCached() {
        return !first.getConfig().temporaryEmpty && !second.getConfig().temporaryEmpty;
    }
}
