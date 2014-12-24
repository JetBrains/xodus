/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.RandomAccessLoggable;
import jetbrains.exodus.tree.IExpirationChecker;
import jetbrains.exodus.tree.ITreeCursor;
import jetbrains.exodus.tree.TreeMetaInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

/**
 * Fake class defining behavior of empty immutable store.
 */
class StoreEmpty extends StoreImpl {

    StoreEmpty(@NotNull final EnvironmentImpl env, final int structureId) {
        super(env, "fake empty store", TreeMetaInfo.EMPTY.clone(structureId));
    }

    @Override
    @Nullable
    public ByteIterable get(@NotNull final Transaction txn, @NotNull final ByteIterable key) {
        return null;
    }

    @Override
    public boolean exists(@NotNull final Transaction txn,
                          @NotNull final ByteIterable key,
                          @NotNull final ByteIterable data) {
        return false;
    }

    @Override
    public boolean put(@NotNull final Transaction txn,
                       @NotNull final ByteIterable key,
                       @NotNull final ByteIterable value) {
        return throwCantModify();
    }

    @Override
    public void putRight(@NotNull final Transaction txn,
                         @NotNull final ByteIterable key,
                         @NotNull final ByteIterable value) {
        throwCantModify();
    }

    @Override
    public boolean add(@NotNull final Transaction txn,
                       @NotNull final ByteIterable key,
                       @NotNull final ByteIterable value) {
        return throwCantModify();
    }

    @Override
    public boolean delete(@NotNull final Transaction txn, @NotNull final ByteIterable key) {
        return throwCantModify();
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
    public void reclaim(@NotNull final Transaction txn,
                        @NotNull final RandomAccessLoggable loggable,
                        @NotNull final Iterator<RandomAccessLoggable> loggables,
                        @NotNull final IExpirationChecker expirationChecker) {
        // nothing to reclaim
    }

    private static boolean throwCantModify() {
        throw new UnsupportedOperationException("Can't modify fake empty store");
    }
}
