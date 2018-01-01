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
package jetbrains.exodus.tree.btree;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.tree.ITreeCursor;
import org.jetbrains.annotations.NotNull;

public class BTreeEmpty extends BTreeBase {

    public BTreeEmpty(@NotNull final Log log,
                      @NotNull final BTreeBalancePolicy balancePolicy,
                      final boolean allowsDuplicates,
                      final int structureId) {
        super(log, balancePolicy, allowsDuplicates, structureId);
        size = 0;
    }

    public BTreeEmpty(@NotNull final Log log, final boolean allowsDuplicates, final int structureId) {
        this(log, BTreeBalancePolicy.DEFAULT, allowsDuplicates, structureId);
    }

    @Override
    @NotNull
    public BTreeMutable getMutableCopy() {
        return new BTreeMutable(this);
    }

    @Override
    public long getRootAddress() {
        return Loggable.NULL_ADDRESS;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public ITreeCursor openCursor() {
        return ITreeCursor.EMPTY_CURSOR;
    }

    @Override
    public boolean hasKey(@NotNull final ByteIterable key) {
        return false;
    }

    @Override
    public boolean hasPair(@NotNull final ByteIterable key, @NotNull final ByteIterable value) {
        return false;
    }

    @NotNull
    @Override
    protected BasePage getRoot() {
        return new BottomPage(this);
    }

}
