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
package jetbrains.exodus.env;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static jetbrains.exodus.env.EnvironmentStatistics.Type.READONLY_TRANSACTIONS;

public class ReadonlyTransaction extends TransactionBase {

    @Nullable
    private final Runnable beginHook;

    public ReadonlyTransaction(@NotNull final EnvironmentImpl env, @Nullable final Runnable beginHook) {
        super(env, false);
        this.beginHook = getWrappedBeginHook(beginHook);
        env.holdNewestSnapshotBy(this);
        env.getStatistics().getStatisticsItem(READONLY_TRANSACTIONS).incTotal();
    }

    /**
     * Constructor for creating new snapshot transaction.
     */
    ReadonlyTransaction(@NotNull final TransactionBase origin) {
        super(origin.getEnvironment(), false);
        beginHook = null;
        setMetaTree(origin.getMetaTree());
        final EnvironmentImpl env = getEnvironment();
        env.acquireTransaction(this);
        env.registerTransaction(this);
        env.getStatistics().getStatisticsItem(READONLY_TRANSACTIONS).incTotal();
    }

    @Override
    public void setCommitHook(@Nullable final Runnable hook) {
        throw new ReadonlyTransactionException();
    }

    @Override
    void storeRemoved(@NotNull final StoreImpl store) {
        throw new ReadonlyTransactionException();
    }

    @Override
    public boolean isIdempotent() {
        return true;
    }

    @Override
    public void abort() {
        checkIsFinished();
        getEnvironment().finishTransaction(this);
    }

    @Override
    public boolean commit() {
        throw new ReadonlyTransactionException();
    }

    @Override
    public boolean flush() {
        throw new ReadonlyTransactionException();
    }

    @Override
    public void revert() {
        throw new ReadonlyTransactionException();
    }

    @Override
    public boolean isReadonly() {
        return true;
    }

    @Nullable
    @Override
    Runnable getBeginHook() {
        return beginHook;
    }
}
