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
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.ITreeCursor;
import jetbrains.exodus.util.ArrayBackedByteIterable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class TreeImmutableCursor implements ITreeCursor {
    private ImmutableCursorState state;

    private final ITree tree;
    protected TraversablePage root;

    public TreeImmutableCursor(final ITree tree, final TraversablePage root) {
        assert root != null;

        this.root = root;
        this.tree = tree;
    }

    @Override
    public boolean getNext() {
        if (state == null) {
            state = new ImmutableCursorState(root);
            return state.isFull();
        }

        return state.next();
    }

    @Override
    public final boolean getNextDup() {
        return getNext();
    }

    @Override
    public final boolean getNextNoDup() {
        return getNext();
    }

    @Override
    public boolean getLast() {
        state = new ImmutableCursorState(root, ImmutableCursorState.Traverse.LAST);
        return state.isFull();
    }

    @Override
    public boolean getPrev() {
        if (state == null) {
            state = new ImmutableCursorState(root, ImmutableCursorState.Traverse.LAST);
            return state.isFull();
        }

        return state.prev();
    }

    @Override
    public final boolean getPrevDup() {
        return getPrev();
    }

    @Override
    public final boolean getPrevNoDup() {
        return getPrev();
    }

    @Override
    public final @NotNull ByteIterable getKey() {
        var result = state.key();

        if (result == null) {
            return ByteIterable.EMPTY;
        }

        return result;
    }


    @Override
    public final @NotNull ByteIterable getValue() {
        var result = state.value();
        if (result == null) {
            return ByteIterable.EMPTY;
        }

        return result;
    }

    @Override
    public final @Nullable ByteIterable getSearchKey(@NotNull ByteIterable key) {
        return findByKey(key);
    }

    @Nullable
    private ByteIterable findByKey(@NotNull ByteIterable key) {
        var page = root;

        if (page.getEntriesCount() == 0) {
            return null;
        }

        if (state == null) {
            state = new ImmutableCursorState(root, ImmutableCursorState.Traverse.NONE);
        }

        if (key instanceof ArrayBackedByteIterable arrayBackedByteIterable) {
            return page.find(arrayBackedByteIterable.duplicate(), state, 0, arrayBackedByteIterable.offset());
        }

        var currentKey = new ArrayBackedByteIterable(key.getBytesUnsafe(), 0, key.getLength());
        return page.find(currentKey, state, 0, currentKey.offset());
    }

    @Override
    public @Nullable ByteIterable getSearchKeyRange(@NotNull ByteIterable key) {
        return findByKeyRange(key);
    }

    @Nullable
    final ByteIterable findByKeyRange(@NotNull ByteIterable key) {
        var page = root;
        if (page.getEntriesCount() == 0) {
            return null;
        }

        if (state == null) {
            state = new ImmutableCursorState(root, ImmutableCursorState.Traverse.NONE);
        }

        ArrayBackedByteIterable currentKey;

        if (key instanceof ArrayBackedByteIterable arrayBackedByteIterable) {
            currentKey = arrayBackedByteIterable;
        } else {
            currentKey = new ArrayBackedByteIterable(key.getBytesUnsafe(), 0, key.getLength());
        }

        return page.findByKeyRange(currentKey, state, 0, false);
    }


    @Override
    public boolean getSearchBoth(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        var foundValue = findByKey(key);
        if (foundValue == null) {
            return false;
        }

        return foundValue.compareTo(value) == 0;
    }

    @Override
    public @Nullable ByteIterable getSearchBothRange(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        var foundValue = findByKeyRange(key);
        if (foundValue == null) {
            return null;
        }

        if (foundValue.compareTo(value) >= 0) {
            return foundValue;
        }

        return null;
    }

    @Override
    public final int count() {
        if (state.isEmpty()) {
            return 0;
        }

        return 1;
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean deleteCurrent() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final ITree getTree() {
        return tree;
    }

    public final void reset() {
        state = null;
    }
}
