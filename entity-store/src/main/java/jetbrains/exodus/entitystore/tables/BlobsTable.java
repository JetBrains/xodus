/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class BlobsTable extends Table {

    private final Store primaryStore;
    private final FieldIndex allBlobsIndex;

    public BlobsTable(@NotNull final PersistentEntityStoreImpl store,
                      @NotNull final PersistentStoreTransaction txn,
                      @NotNull final String name,
                      @NotNull final StoreConfig primaryConfig) {
        final Transaction envTxn = txn.getEnvironmentTransaction();
        final Environment env = store.getEnvironment();
        primaryStore = env.openStore(name, primaryConfig, envTxn);
        allBlobsIndex = FieldIndex.fieldIndex(txn, name);
        store.trackTableCreation(primaryStore, txn);
    }

    @Nullable
    public ByteIterable get(@NotNull final Transaction txn, @NotNull final PropertyKey propertyKey) {
        return primaryStore.get(txn, PropertyKey.propertyKeyToEntry(propertyKey));
    }

    @Nullable
    public ByteIterable get(@NotNull final Transaction txn, final long localId, final int blobId) {
        return get(txn, new PropertyKey(localId, blobId));
    }

    /**
     * Setter for blob handle value.
     *
     * @param txn     enclosing transaction
     * @param localId entity local id.
     * @param blobId  blob id
     * @param value   property value.
     */
    public void put(@NotNull final Transaction txn, final long localId,
                    final int blobId, @NotNull final ByteIterable value) {
        primaryStore.put(txn, PropertyKey.propertyKeyToEntry(new PropertyKey(localId, blobId)), value);
        allBlobsIndex.put(txn, blobId, localId);
    }

    public void delete(@NotNull final Transaction txn, final long localId, final int blobId) {
        final ByteIterable key = PropertyKey.propertyKeyToEntry(new PropertyKey(localId, blobId));
        boolean success = primaryStore.delete(txn, key) && allBlobsIndex.remove(txn, blobId, localId);
        checkStatus(success, "Failed to delete");
    }

    public Store getPrimaryIndex() {
        return primaryStore;
    }

    public FieldIndex getAllBlobsIndex() {
        return allBlobsIndex;
    }

    @Override
    public boolean canBeCached() {
        return !primaryStore.getConfig().temporaryEmpty && !allBlobsIndex.getStore().getConfig().temporaryEmpty;
    }
}
