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

import jetbrains.exodus.env.Transaction;
import org.jetbrains.annotations.NotNull;

final class PersistentStoreTransactionSnapshot extends PersistentStoreTransaction {

    PersistentStoreTransactionSnapshot(@NotNull final PersistentStoreTransaction source,
                                       @NotNull final Transaction txn) {
        super(source, txn.getSnapshot());
    }

    @Override
    public boolean isCurrent() {
        return false;
    }

    @Override
    public boolean flush() {
        throw new IllegalStateException("Can't flush historical snapshot!");
    }

    @Override
    public boolean commit() {
        throw new IllegalStateException("Can't commit historical snapshot!");
    }

    @Override
    public void abort() {
        try {
            disposeCreatedIterators();
            revertCaches();
        } finally {
            txn.abort();
        }
    }

    @Override
    public void revert() {
        throw new IllegalStateException("Can't revert historical snapshot!");
    }
}
