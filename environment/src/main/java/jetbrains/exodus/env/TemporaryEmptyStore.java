/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.env;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.RandomAccessLoggable;
import jetbrains.exodus.tree.ITreeCursor;
import jetbrains.exodus.tree.TreeMetaInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

class TemporaryEmptyStore extends StoreImpl {

    TemporaryEmptyStore(@NotNull final EnvironmentImpl env, @NotNull final String name) {
        super(env, name, TreeMetaInfo.EMPTY.clone(-1));
    }

    @NotNull
    @Override
    public StoreConfig getConfig() {
        return StoreConfig.TEMPORARY_EMPTY;
    }

    TemporaryEmptyStore(@NotNull final EnvironmentImpl env) {
        this(env, "Temporary Empty Store");
    }

    @Override
    @Nullable
    public ByteIterable get(@NotNull final Transaction txn, @NotNull final ByteIterable key) {
        return null;
    }

    @Override
    public boolean exists(@NotNull final Transaction txn,
                          @NotNull final ByteIterable key,
                          @NotNull final ByteIterable value) {
        return false;
    }

    @Override
    public boolean put(@NotNull final Transaction txn,
                       @NotNull final ByteIterable key,
                       @NotNull final ByteIterable value) {
        return throwCantModify(txn);
    }

    @Override
    public void putRight(@NotNull final Transaction txn,
                         @NotNull final ByteIterable key,
                         @NotNull final ByteIterable value) {
        throwCantModify(txn);
    }

    @Override
    public boolean add(@NotNull final Transaction txn,
                       @NotNull final ByteIterable key,
                       @NotNull final ByteIterable value) {
        return throwCantModify(txn);
    }

    @Override
    public boolean delete(@NotNull final Transaction txn, @NotNull final ByteIterable key) {
        return throwCantModify(txn);
    }

    @Override
    public long count(@NotNull final Transaction txn) {
        return 0;
    }

    @Override
    public Cursor openCursor(@NotNull final Transaction txn) {
        return ITreeCursor.EMPTY_CURSOR;
    }

    @Override
    public void reclaim(@NotNull final Transaction transaction,
                        @NotNull final RandomAccessLoggable loggable,
                        @NotNull final Iterator<RandomAccessLoggable> loggables) {
        // nothing to reclaim
    }

    @Override
    public void reclaimByTreeIteration(@NotNull Transaction transaction, long startAddress, long endAddress) {
        // nothing to reclaim
    }

    private boolean throwCantModify(Transaction txn) {
        if (txn.isReadonly()) {
            throw new ReadonlyTransactionException();
        }
        throw new UnsupportedOperationException("Can't modify temporary empty store");
    }
}
