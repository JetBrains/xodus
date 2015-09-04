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
package jetbrains.exodus.env;

import jetbrains.exodus.tree.ITreeMutable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class ReadonlyTransaction extends TransactionImpl {

    ReadonlyTransaction(@NotNull final EnvironmentImpl env,
                        @Nullable final Thread creatingThread,
                        @Nullable final Runnable beginHook) {
        super(env, creatingThread, beginHook, false, false);
    }

    /**
     * Constructor for creating new snapshot transaction.
     */
    ReadonlyTransaction(@NotNull final TransactionImpl origin) {
        super(origin);
    }

    @Override
    public Transaction getSnapshot() {
        return this;
    }

    @Override
    void storeRemoved(@NotNull final StoreImpl store) {
        throw new ReadonlyTransactionException();
    }

    @Override
    void storeCreated(@NotNull final StoreImpl store) {
        throw new ReadonlyTransactionException();
    }

    @NotNull
    @Override
    ITreeMutable getMutableTree(@NotNull final StoreImpl store) {
        throw new ReadonlyTransactionException();
    }

    @Override
    public boolean isIdempotent() {
        if (!super.isIdempotent()) {
            throw new IllegalStateException("ReadonlyTransaction should be idempotent");
        }
        return true;
    }

    @Override
    public boolean isReadonly() {
        return true;
    }

    @Override
    public boolean isExclusive() {
        return false;
    }
}
