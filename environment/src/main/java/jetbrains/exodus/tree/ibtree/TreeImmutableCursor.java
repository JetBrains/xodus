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

import it.unimi.dsi.fastutil.objects.ObjectArrayFIFOQueue;
import jetbrains.exodus.ByteBufferByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.ITreeCursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public class TreeImmutableCursor implements ITreeCursor {
    private ObjectArrayFIFOQueue<ElemRef> stack = new ObjectArrayFIFOQueue<>(8);
    private boolean initiated = false;

    private final ITree tree;
    protected TraversablePage root;

    public TreeImmutableCursor(final ITree tree, final TraversablePage root) {
        assert root != null;

        this.root = root;
        this.tree = tree;
    }

    private void initStack() {
        var page = root;
        if (page.getEntriesCount() > 0) {
            var pageRef = new ElemRef(page, 0);
            stack.enqueue(pageRef);

            downToTheFirstEntry(pageRef);
        }
    }

    private void downToTheFirstEntry(ElemRef elemRef) {
        var page = elemRef.page;
        if (page.isInternalPage()) {
            var childIndex = elemRef.index;

            page = page.child(childIndex);
            stack.enqueue(new ElemRef(page, 0));

            while (page.isInternalPage()) {
                page = page.child(0);
                stack.enqueue(new ElemRef(page, 0));
            }
        }

    }

    private void downToTheLastEntry(ElemRef elemRef) {
        var page = elemRef.page;
        if (page.isInternalPage()) {
            var childIndex = elemRef.index;
            page = page.child(childIndex);

            while (page.isInternalPage()) {
                var index = page.getEntriesCount() - 1;
                page = page.child(index);

                stack.enqueue(new ElemRef(page, childIndex));
            }
        }
    }

    @Override
    public boolean getNext() {
        if (!initiated) {
            assert stack.isEmpty();
            initStack();

            initiated = true;

            return !stack.isEmpty();
        }

        if (stack.isEmpty()) {
            return false;
        }

        var currentRef = stack.last();
        var index = currentRef.index + 1;

        if (index < currentRef.page.getEntriesCount()) {
            currentRef.index = index;
            return true;
        }

        while (true) {
            stack.dequeue();

            if (stack.isEmpty()) {
                return false;
            }

            var last = stack.last();
            var lastIndex = last.index + 1;

            if (lastIndex < last.page.getEntriesCount()) {
                last.index = lastIndex;

                downToTheFirstEntry(last);
                return true;
            }
        }
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
        initiated = true;

        stack.clear();

        var page = root;
        var rootSize = page.getEntriesCount();

        if (rootSize == 0) {
            return false;
        }

        var rootRef = new ElemRef(page, rootSize - 1);
        stack.enqueue(rootRef);

        downToTheLastEntry(rootRef);

        return true;
    }

    @Override
    public boolean getPrev() {
        if (!initiated) {
            return getLast();
        }

        if (stack.isEmpty()) {
            return false;
        }

        var currentRef = stack.last();
        var index = currentRef.index - 1;

        if (index >= 0) {
            currentRef.index = index;
            return true;
        }

        while (true) {
            stack.dequeue();

            if (stack.isEmpty()) {
                return false;
            }

            var last = stack.last();
            var lastIndex = last.index - 1;

            if (lastIndex >= 0) {
                last.index = lastIndex;

                downToTheLastEntry(last);
                return true;
            }
        }
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
        var key = doGetKey();
        if (key == null) {
            return ByteIterable.EMPTY;
        }

        return new ByteBufferByteIterable(key);
    }

    final ByteBuffer doGetKey() {
        if (stack.isEmpty()) {
            return null;
        }

        var last = stack.last();
        var page = last.page;

        assert !page.isInternalPage();

        return page.key(last.index);
    }


    @Override
    public final @NotNull ByteIterable getValue() {
        if (stack.isEmpty()) {
            return ByteIterable.EMPTY;
        }

        var last = stack.last();
        var page = last.page;

        assert !page.isInternalPage();

        return new ByteBufferByteIterable(page.value(last.index));
    }

    @Override
    public @Nullable ByteIterable getSearchKey(@NotNull ByteIterable key) {
        return findByKey(key);
    }

    @Nullable
    private ByteBufferByteIterable findByKey(@NotNull ByteIterable key) {
        var page = root;
        if (page.getEntriesCount() == 0) {
            return null;
        }

        var stackBackup = stack;
        stack = new ObjectArrayFIFOQueue<>();

        var keyBuffer = key.getByteBuffer();

        while (true) {
            var index = page.find(keyBuffer);
            if (!page.isInternalPage()) {
                if (index >= 0) {
                    stack.enqueue(new ElemRef(page, index));
                    return new ByteBufferByteIterable(page.value(index));
                } else {
                    stack = stackBackup;
                    return null;
                }
            } else {
                if (index < 0) {
                    index = -index - 2;
                    if (index < 0) {
                        stack = stackBackup;
                        return null;
                    } else {
                        stack.enqueue(new ElemRef(page, index));
                        page = page.child(index);
                    }
                }
            }
        }
    }

    @Override
    public @Nullable ByteIterable getSearchKeyRange(@NotNull ByteIterable key) {
        return findByKeyRange(key.getByteBuffer());
    }

    @Nullable
    final ByteBufferByteIterable findByKeyRange(@NotNull ByteBuffer key) {
        var page = root;
        if (page.getEntriesCount() == 0) {
            return null;
        }

        var stackBackup = stack;
        stack = new ObjectArrayFIFOQueue<>();

        while (true) {
            var index = page.find(key);

            if (!page.isInternalPage()) {
                if (index >= 0) {
                    stack.enqueue(new ElemRef(page, index));
                    return new ByteBufferByteIterable(page.value(index));
                } else {
                    index = -index - 1;
                    if (index >= page.getEntriesCount()) {
                        stack = stackBackup;
                        return null;
                    }
                    stack.enqueue(new ElemRef(page, index));
                    return new ByteBufferByteIterable(page.value(index));
                }
            } else {
                if (index < 0) {
                    index = -index - 1;
                    if (index > 0) {
                        index--;
                    }
                    stack.enqueue(new ElemRef(page, index));
                    page = page.child(index);
                }
            }
        }
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
        var foundValue = findByKeyRange(key.getByteBuffer());
        if (foundValue == null) {
            return null;
        }

        if (value instanceof ByteBufferByteIterable) {
            if (foundValue.getByteBuffer().compareTo(value.getByteBuffer()) >= 0) {
                return foundValue;
            }
        } else {
            if (foundValue.compareTo(value) >= 0) {
                return foundValue;
            }
        }

        return null;
    }

    @Override
    public final int count() {
        if (stack.isEmpty()) {
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
        initiated = false;
        stack.clear();
    }

    private static final class ElemRef {
        private int index;
        private final TraversablePage page;

        private ElemRef(TraversablePage page, int index) {
            this.index = index;
            this.page = page;
        }
    }
}
