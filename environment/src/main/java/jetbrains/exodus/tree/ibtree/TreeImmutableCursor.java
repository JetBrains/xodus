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
import jetbrains.exodus.ByteBufferComparator;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.tree.ITree;
import jetbrains.exodus.tree.ITreeCursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public class TreeImmutableCursor implements ITreeCursor {
    private ObjectArrayFIFOQueue<ElemRef> stack = new ObjectArrayFIFOQueue<>(8);
    private boolean initialized = false;

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
            var lastIndex = page.getEntriesCount() - 1;
            stack.enqueue(new ElemRef(page, lastIndex));

            while (page.isInternalPage()) {
                page = page.child(lastIndex);

                lastIndex = page.getEntriesCount() - 1;
                stack.enqueue(new ElemRef(page, lastIndex));
            }
        }
    }

    @Override
    public boolean getNext() {
        if (!initialized) {
            assert stack.isEmpty();
            initStack();

            initialized = true;

            if (!stack.isEmpty()) {
                var last = stack.last();

                //if we found empty leaf page, we need to move to the next one
                if (last.page.getEntriesCount() > 0) {
                    return true;
                }
            } else {
                return false;
            }
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
            var page = moveToTheNextNotVisitedDepthFirstPage();
            if (page == null) {
                return false;
            }

            var elemRef = new ElemRef(page, 0);
            stack.enqueue(elemRef);

            downToTheFirstEntry(elemRef);

            var last = stack.last();
            if (last.page.getEntriesCount() > 0) {
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
        initialized = true;

        stack.clear();

        var page = root;
        var rootSize = page.getEntriesCount();

        if (rootSize == 0) {
            return false;
        }

        var elemRef = new ElemRef(page, rootSize - 1);
        stack.enqueue(elemRef);

        while (true) {
            downToTheLastEntry(elemRef);

            var last = stack.last();
            if (last.page.getEntriesCount() > 0) {
                return true;
            }

            page = moveToThePrevNotVisitedDepthFirstPage();
            if (page == null) {
                return false;
            }

            elemRef = new ElemRef(page, page.getEntriesCount() - 1);
            stack.enqueue(elemRef);
        }
    }

    @Override
    public boolean getPrev() {
        if (!initialized) {
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
            var page = moveToThePrevNotVisitedDepthFirstPage();
            if (page == null) {
                return false;
            }

            var elemRef = new ElemRef(page, page.getEntriesCount() - 1);
            stack.enqueue(elemRef);

            downToTheLastEntry(elemRef);

            var last = stack.last();
            if (last.page.getEntriesCount() > 0) {
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
        var result = getKeyBuffer();

        if (result == null) {
            return ByteIterable.EMPTY;
        }

        return new ByteBufferByteIterable(result);
    }

    @Override
    public final ByteBuffer getKeyBuffer() {
        return doGetKey();
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
        var result = getValueBuffer();
        if (result == null) {
            return ByteIterable.EMPTY;
        }

        return new ByteBufferByteIterable(result);
    }


    @Override
    public final ByteBuffer getValueBuffer() {
        if (stack.isEmpty()) {
            return null;
        }

        var last = stack.last();
        var page = last.page;

        assert !page.isInternalPage();

        return page.value(last.index);
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
        initialized = true;

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
                    return page.value(index);
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
                    }
                }


                stack.enqueue(new ElemRef(page, index));
                page = page.child(index);
            }
        }
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
        initialized = true;

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
                    return page.value(index);
                } else {
                    index = -index - 1;
                    if (index >= page.getEntriesCount()) {
                        page = moveToTheNextNotVisitedDepthFirstPage();

                        if (page == null) {
                            stack = stackBackup;
                            return null;
                        }
                    } else {
                        stack.enqueue(new ElemRef(page, index));

                        return page.value(index);
                    }
                }
            } else {
                if (index < 0) {
                    index = -index - 1;

                    if (index > 0) {
                        index--;
                    }
                }

                stack.enqueue(new ElemRef(page, index));
                page = page.child(index);
            }
        }
    }

    /**
     * Goes up by the stack till it does not find ancestor which elements were not completely visited in forward
     * direction.
     * And fetches first page pointed by element which goes next to the last visited element
     * without putting it to the stack.
     */
    private TraversablePage moveToTheNextNotVisitedDepthFirstPage() {
        if (stack.isEmpty()) {
            return null;
        }

        var elemRef = stack.last();
        var page = elemRef.page;
        var index = elemRef.index;

        while (true) {
            if (index < page.getEntriesCount() - 1) {
                index++;
                elemRef.index = index;
                return page.child(index);
            }

            stack.dequeueLast();

            if (!stack.isEmpty()) {
                elemRef = stack.last();
                page = elemRef.page;
                index = elemRef.index;
            } else {
                return null;
            }
        }
    }

    /**
     * Goes up by the stack till it does not find ancestor which elements were not completely visited in backward
     * direction.
     * And fetches closest page pointed by element which goes next in backward direction to the last visited element
     * without putting it to the stack.
     */
    private TraversablePage moveToThePrevNotVisitedDepthFirstPage() {
        if (stack.isEmpty()) {
            return null;
        }

        var elemRef = stack.last();
        var page = elemRef.page;
        var index = elemRef.index;

        while (true) {
            if (index > 0) {
                index--;
                elemRef.index = index;
                return page.child(index);
            }

            stack.dequeueLast();

            if (!stack.isEmpty()) {
                elemRef = stack.last();
                page = elemRef.page;
                index = elemRef.index;
            } else {
                return null;
            }
        }
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
        initialized = false;
        stack.clear();
    }

    private static final class ElemRef {
        private int index;
        private final TraversablePage page;

        private ElemRef(TraversablePage page, int index) {
            assert page != null;

            this.index = index;
            this.page = page;
        }
    }
}
