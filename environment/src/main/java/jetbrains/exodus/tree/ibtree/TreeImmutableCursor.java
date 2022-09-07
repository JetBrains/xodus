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

import jetbrains.exodus.ByteBufferByteIterable;
import jetbrains.exodus.ByteBufferComparator;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.ITreeCursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

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
        var result = getKeyBuffer();

        if (result == null) {
            return ByteIterable.EMPTY;
        }

        return new ByteBufferByteIterable(result);
    }

    @Override
    public final ByteBuffer getKeyBuffer() {
        if (state == null) {
            return null;
        }

        return state.key();
    }


    @Override
    public final @NotNull ByteIterable getValue() {
        var result = getValueBuffer();
        if (result == null) {
            return ByteIterable.EMPTY;
        }

        return new ByteBufferByteIterable(result);
    }


    @Override
    public final ByteBuffer getValueBuffer() {
        if (state == null) {
            return null;
        }

        return state.value();
    }

    @Override
    public final @Nullable ByteIterable getSearchKey(@NotNull ByteIterable key) {
        var result = getSearchKey(key.getByteBuffer());
        if (result == null) {
            return null;
        }

        return new ByteBufferByteIterable(result);
    }

    @Override
    public @Nullable ByteBuffer getSearchKey(@NotNull ByteBuffer key) {
        return findByKey(key);
    }

    @Nullable
    private ByteBuffer findByKey(@NotNull ByteBuffer key) {
        var page = root;
        if (page.getEntriesCount() == 0) {
            return null;
        }

        if (state == null) {
            state = new ImmutableCursorState(root, ImmutableCursorState.Traverse.NONE);
        }

        return page.find(key.duplicate(), state, 0);
    }

    @Override
    public final @Nullable ByteIterable getSearchKeyRange(@NotNull ByteIterable key) {
        var result = getSearchKeyRange(key.getByteBuffer());
        if (result == null) {
            return null;
        }

        return new ByteBufferByteIterable(result);
    }

    @Override
    public @Nullable ByteBuffer getSearchKeyRange(@NotNull ByteBuffer key) {
        return findByKeyRange(key);
    }

    @Nullable
    final ByteBuffer findByKeyRange(@NotNull ByteBuffer key) {
        var page = root;
        if (page.getEntriesCount() == 0) {
            return null;
        }

        if (state == null) {
            state = new ImmutableCursorState(root, ImmutableCursorState.Traverse.NONE);
        }

        return page.findByKeyRange(key.duplicate(), state, 0, false);
    }


    @Override
    public final boolean getSearchBoth(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        return getSearchBoth(key.getByteBuffer(), value.getByteBuffer());
    }

    @Override
    public boolean getSearchBoth(@NotNull ByteBuffer key, @NotNull ByteBuffer value) {
        var foundValue = findByKey(key);
        if (foundValue == null) {
            return false;
        }

        return ByteBufferComparator.INSTANCE.compare(foundValue, value) == 0;
    }

    @Override
    public final @Nullable ByteIterable getSearchBothRange(@NotNull ByteIterable key, @NotNull ByteIterable value) {
        var result = getSearchBothRange(key.getByteBuffer(), value.getByteBuffer());
        if (result != null) {
            return new ByteBufferByteIterable(result);
        }

        return null;
    }

    @Override
    public @Nullable ByteBuffer getSearchBothRange(@NotNull ByteBuffer key, @NotNull ByteBuffer value) {
        var foundValue = findByKeyRange(key);
        if (foundValue == null) {
            return null;
        }

        if (ByteBufferComparator.INSTANCE.compare(foundValue, value) >= 0) {
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
