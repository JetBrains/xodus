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
package jetbrains.exodus.tree.patricia;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.Loggable;
import jetbrains.exodus.tree.ITreeCursor;
import jetbrains.exodus.tree.ITreeMutable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class PatriciaTreeEmpty extends PatriciaTreeBase {

    private final boolean hasDuplicates;

    public PatriciaTreeEmpty(@NotNull final Log log, final int structureId, final boolean hasDuplicates) {
        super(log, structureId);
        size = 0;
        this.hasDuplicates = hasDuplicates;
    }

    @NotNull
    @Override
    public ITreeMutable getMutableCopy() {
        final PatriciaTreeMutable treeMutable = new PatriciaTreeMutable(log, structureId, 0, getRoot());
        return hasDuplicates ? new PatriciaTreeWithDuplicatesMutable(treeMutable) : treeMutable;
    }

    @Override
    public long getRootAddress() {
        return Loggable.NULL_ADDRESS;
    }

    @Override
    public ITreeCursor openCursor() {
        return ITreeCursor.EMPTY_CURSOR;
    }

    @Override
    ImmutableNode getRoot() {
        return new ImmutableNode();
    }

    @Nullable
    @Override
    protected NodeBase getNode(@NotNull final ByteIterable key) {
        return null;
    }
}
