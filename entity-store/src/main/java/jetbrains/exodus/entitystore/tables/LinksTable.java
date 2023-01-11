/**
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
package jetbrains.exodus.entitystore.tables;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.entitystore.PersistentStoreTransaction;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import org.jetbrains.annotations.NotNull;

public final class LinksTable extends TwoColumnTable {

    private final FieldIndex allLinksIndex;

    public LinksTable(@NotNull PersistentStoreTransaction txn, @NotNull String name, @NotNull StoreConfig config) {
        super(txn, name, config);
        allLinksIndex = FieldIndex.fieldIndex(txn, name);
    }

    public boolean put(@NotNull Transaction txn,
                       final long localId,
                       @NotNull ByteIterable value,
                       boolean noOldValue,
                       final int linkId) {
        final PropertyKey linkKey = new PropertyKey(localId, linkId);
        boolean success = super.put(txn, PropertyKey.propertyKeyToEntry(linkKey), value);
        if (noOldValue) {
            success |= allLinksIndex.put(txn, linkId, localId);
        }
        return success;
    }

    public boolean delete(@NotNull Transaction txn,
                          final long localId,
                          @NotNull ByteIterable value,
                          boolean noNewValue,
                          final int linkId) {
        PropertyKey linkKey = new PropertyKey(localId, linkId);
        boolean success = super.delete(txn, PropertyKey.propertyKeyToEntry(linkKey), value);
        if (noNewValue) {
            success |= deleteAllIndex(txn, linkId, localId);
        }
        return success;
    }

    public boolean deleteAllIndex(@NotNull Transaction txn, int linkId, long localId) {
        return allLinksIndex.remove(txn, linkId, localId);
    }

    public FieldIndex getAllLinksIndex() {
        return allLinksIndex;
    }

    @Override
    public boolean canBeCached() {
        return super.canBeCached() && !allLinksIndex.getStore().getConfig().temporaryEmpty;
    }
}
