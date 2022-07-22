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

public final class TreeImmutableCursor implements ITreeCursor {
    private final ImmutableBTree tree;
    private ObjectArrayFIFOQueue<ElemRef> stack = new ObjectArrayFIFOQueue<>();
    private boolean initiated = false;

    public TreeImmutableCursor(ImmutableBTree tree) {
        assert tree.root != null;
        this.tree = tree;
    }

    private void initStack() {
        assert tree.root != null;

        var page = tree.root;
        if (page.getEntriesCount() > 0) {
            var pageRef = new ElemRef(page, 0);
            stack.enqueue(pageRef);

            downToTheFirstEntry(pageRef);
        }
    }

    private void downToTheFirstEntry(ElemRef elemRef) {
        var page = elemRef.page;
        if (page instanceof ImmutableInternalPage) {
            var childIndex = elemRef.index;

            var childAddress = page.getChildAddress(childIndex);
            page = tree.loadPage(childAddress);
            stack.enqueue(new ElemRef(page, 0));

            while (page instanceof ImmutableInternalPage) {
                childAddress = page.getChildAddress(0);
                page = tree.loadPage(childAddress);
                stack.enqueue(new ElemRef(page, 0));
            }
        }

    }

    private void downToTheLastEntry(ElemRef elemRef) {
        var page = elemRef.page;
        if (page instanceof ImmutableInternalPage) {
            var childIndex = elemRef.index;

            var childAddress = page.getChildAddress(childIndex);
            page = tree.loadPage(childAddress);

            while (page instanceof ImmutableInternalPage) {
                var index = page.getEntriesCount() - 1;
                childAddress = page.getChildAddress(index);
                page = tree.loadPage(childAddress);

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
    public boolean getNextDup() {
        return getNext();
    }

    @Override
    public boolean getNextNoDup() {
        return getNext();
    }

    @Override
    public boolean getLast() {
        initiated = true;

        stack.clear();

        assert tree.root != null;

        var page = tree.root;
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
        if (initiated) {
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
    public boolean getPrevDup() {
        return getPrev();
    }

    @Override
    public boolean getPrevNoDup() {
        return getPrev();
    }

    @Override
    public @NotNull ByteIterable getKey() {
        if (stack.isEmpty()) {
            return ByteIterable.EMPTY;
        }

        var last = stack.last();
        var page = (ImmutableLeafPage) last.page;

        return new ByteBufferByteIterable(page.keyView.get(last.index));
    }

    @Override
    public @NotNull ByteIterable getValue() {
        if (stack.isEmpty()) {
            return ByteIterable.EMPTY;
        }

        var last = stack.last();
        var page = (ImmutableLeafPage) last.page;

        return new ByteBufferByteIterable(page.getValue(last.index));
    }

    @Override
    public @Nullable ByteIterable getSearchKey(@NotNull ByteIterable key) {
        return findByKey(key);
    }

    @Nullable
    private ByteBufferByteIterable findByKey(@NotNull ByteIterable key) {
        assert tree.root != null;

        var page = tree.root;
        if (page.getEntriesCount() == 0) {
            return null;
        }

        var stackBackup = stack;
        stack = new ObjectArrayFIFOQueue<>();

        var keyBuffer = key.getByteBuffer();

        while (true) {
            var index = page.find(keyBuffer);

            if (page instanceof ImmutableLeafPage) {
                if (index >= 0) {
                    stack.enqueue(new ElemRef(page, index));

                    return new ByteBufferByteIterable(((ImmutableLeafPage) page).getValue(index));
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

                        var childAddress = page.getChildAddress(index);
                        page = tree.loadPage(childAddress);
                    }
                }
            }
        }
    }

    @Override
    public @Nullable ByteIterable getSearchKeyRange(@NotNull ByteIterable key) {
        return findByKeyRange(key);
    }

    @Nullable
    private ByteBufferByteIterable findByKeyRange(@NotNull ByteIterable key) {
        assert tree.root != null;

        var page = tree.root;
        if (page.getEntriesCount() == 0) {
            return null;
        }

        var stackBackup = stack;
        stack = new ObjectArrayFIFOQueue<>();

        var keyBuffer = key.getByteBuffer();

        while (true) {
            var index = page.find(keyBuffer);

            if (page instanceof ImmutableLeafPage) {
                if (index >= 0) {
                    stack.enqueue(new ElemRef(page, index));

                    return new ByteBufferByteIterable(((ImmutableLeafPage) page).getValue(index));
                } else {
                    index = -index - 1;

                    if (index >= page.getEntriesCount()) {
                        stack = stackBackup;
                        return null;
                    }

                    stack.enqueue(new ElemRef(page, index));

                    return new ByteBufferByteIterable(((ImmutableLeafPage) page).getValue(index));
                }
            } else {
                if (index < 0) {
                    index = -index - 1;

                    if (index > 0) {
                        index--;
                    }

                    stack.enqueue(new ElemRef(page, index));

                    var childAddress = page.getChildAddress(index);
                    page = tree.loadPage(childAddress);
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
        var foundValue = findByKeyRange(key);
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
    public int count() {
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
    public ITree getTree() {
        return tree;
    }

    private static final class ElemRef {
        private int index;
        private final ImmutableBasePage page;

        private ElemRef(ImmutableBasePage page, int index) {
            this.index = index;
            this.page = page;
        }
    }
}
