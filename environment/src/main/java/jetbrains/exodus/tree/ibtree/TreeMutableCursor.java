/*
 * *
 *  * Copyright 2010 - 2022 JetBrains s.r.o.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package jetbrains.exodus.tree.ibtree;


import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.tree.ITreeCursorMutable;
import jetbrains.exodus.tree.ITreeMutable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public final class TreeMutableCursor extends TreeImmutableCursor implements ITreeCursorMutable {
    private ByteIterable currentKey = null;
    private final ITreeMutable tree;

    public TreeMutableCursor(ITreeMutable tree, TraversablePage root) {
        super(tree, root);

        this.tree = tree;
    }

    @Override
    public boolean getNext() {
        var result = super.getNext();
        if (result) {
            currentKey = getKey();
        }

        return result;
    }

    @Override
    public boolean getLast() {
        var result = super.getLast();
        if (result) {
            currentKey = getKey();
        }

        return result;
    }

    @Override
    public boolean getPrev() {
        var result = super.getPrev();

        if (result) {
            currentKey = getKey();
        }

        return result;
    }

    @Override
    public @Nullable ByteBuffer getSearchKey(@NotNull ByteBuffer key) {
        var result = super.getSearchKey(key);
        if (result != null) {
            currentKey = getKey();
        }

        return result;
    }

    @Override
    public @Nullable ByteIterable getSearchKeyRange(@NotNull ByteIterable key) {
        var result = super.getSearchKeyRange(key);
        if (result != null) {
            currentKey = getKey();
        }

        return result;
    }

    @Override
    public boolean getSearchBoth(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        var result = super.getSearchBoth(key, value);
        if (result) {
            currentKey = getKey();
        }

        return result;
    }

    @Override
    public @Nullable ByteIterable getSearchBothRange(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        var result = super.getSearchBothRange(key, value);
        if (result != null) {
            currentKey = getKey();
        }

        return result;
    }

    @Override
    public void treeChanged() {
        reset();

        if (currentKey != null) {
            var result = findByKeyRange(currentKey);
            if (result == null) {
                currentKey = null;
                return;
            }

            currentKey = result;
        }
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public void close() {
        tree.cursorClosed(this);
    }

    @Override
    public boolean deleteCurrent() {
        return tree.delete(currentKey, null, this);
    }

    static void notifyCursors(ITreeMutable tree, ITreeCursorMutable cursorToSkip) {
        final Iterable<ITreeCursorMutable> openCursors = tree.getOpenCursors();
        if (openCursors != null) {
            for (ITreeCursorMutable cursor : openCursors) {
                if (cursor != cursorToSkip) {
                    cursor.treeChanged();
                }
            }
        }
    }

    static void notifyCursors(ITreeMutable tree) {
        final Iterable<ITreeCursorMutable> openCursors = tree.getOpenCursors();
        if (openCursors != null) {
            for (ITreeCursorMutable cursor : openCursors) {
                cursor.treeChanged();
            }
        }
    }
}
