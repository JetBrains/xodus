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
import jetbrains.exodus.util.MathUtil;

import java.util.Arrays;

final class ImmutableCursorState {
    private ElemRef[] stack = new ElemRef[8];
    private int stackSize = 0;

    public ImmutableCursorState(final TraversablePage root, final Traverse traverse) {
        var entriesCount = root.getEntriesCount();
        if (entriesCount == 0) {
            return;
        }

        switch (traverse) {
            case LAST -> {
                addToStack(root, entriesCount - 1);
                downToTheLastEntry();
            }
            case FIRST -> {
                addToStack(root, 0);
                downToTheFirstEntry();
            }
            case NONE -> {
            }
        }
    }

    public ImmutableCursorState(TraversablePage root) {
        this(root, Traverse.FIRST);
    }

    private void downToTheFirstEntry() {
        if (stackSize == 0) {
            return;
        }

        var ref = stack[stackSize - 1];

        var page = ref.page;
        var childIndex = ref.childIndex;

        if (page.isInternalPage()) {
            page = page.child(childIndex);

            addToStack(page, 0);
            while (page.isInternalPage()) {
                page = page.child(0);
                addToStack(page, 0);
            }
        }
    }

    public boolean prev() {
        if (stackSize == 0) {
            return false;
        }

        var currentRef = stack[stackSize - 1];
        var index = currentRef.childIndex - 1;

        if (index >= 0) {
            currentRef.childIndex = index;
            return true;
        }

        while (true) {
            var page = moveToThePrevNotVisitedDepthFirstPage();
            if (page == null) {
                return false;
            }

            addToStack(page, page.getEntriesCount() - 1);
            downToTheLastEntry();

            var last = stack[stackSize - 1];
            if (last.page.getEntriesCount() > 0) {
                return true;
            }
        }
    }

    private void downToTheLastEntry() {
        if (stackSize == 0) {
            return;
        }

        var ref = stack[stackSize - 1];

        var page = ref.page;
        var childIndex = ref.childIndex;

        if (page.isInternalPage()) {
            page = page.child(childIndex);
            var lastIndex = page.getEntriesCount() - 1;
            addToStack(page, lastIndex);

            while (page.isInternalPage()) {
                page = page.child(lastIndex);

                lastIndex = page.getEntriesCount() - 1;
                addToStack(page, lastIndex);
            }
        }
    }

    public boolean next() {
        if (stackSize == 0) {
            return false;
        }

        var currentRef = stack[stackSize - 1];
        assert currentRef != null;

        var index = currentRef.childIndex + 1;

        if (index < currentRef.page.getEntriesCount()) {
            currentRef.childIndex = index;

            return true;
        }

        while (true) {
            var page = moveToTheNextNotVisitedDepthFirstPage();

            if (page == null) {
                return false;
            }

            addToStack(page, 0);
            downToTheFirstEntry();

            var last = stack[stackSize - 1];
            assert last != null;

            if (last.page.getEntriesCount() > 0) {
                return true;
            }
        }
    }

    public ByteIterable value() {
        if (stackSize == 0) {
            return null;
        }

        var ref = stack[stackSize - 1];
        return ref.page.value(ref.childIndex);
    }

    public ByteIterable key() {
        if (stackSize == 0) {
            return null;
        }

        var ref = stack[stackSize - 1];
        return ref.page.fullKey(ref.childIndex);
    }


    /**
     * Goes up by the stack till it does not find ancestor which elements were not completely visited in forward
     * direction.
     * And fetches first page pointed by element which goes next to the last visited element
     * without putting it to the stack.
     */
    private TraversablePage moveToTheNextNotVisitedDepthFirstPage() {
        if (stackSize == 0) {
            return null;
        }

        var elemRef = stack[stackSize - 1];
        assert elemRef != null;

        var page = elemRef.page;
        var index = elemRef.childIndex;

        while (true) {
            if (index < page.getEntriesCount() - 1) {
                index++;
                elemRef.childIndex = index;

                return page.child(index);
            }

            removeFromStack();

            if (stackSize > 0) {
                elemRef = stack[stackSize - 1];
                assert elemRef != null;

                page = elemRef.page;
                index = elemRef.childIndex;
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
        if (stackSize == 0) {
            return null;
        }

        var elemRef = stack[stackSize - 1];
        var page = elemRef.page;
        var index = elemRef.childIndex;

        while (true) {
            if (index > 0) {
                index--;
                elemRef.childIndex = index;

                return page.child(index);
            }

            removeFromStack();

            if (stackSize > 0) {
                elemRef = stack[stackSize - 1];
                page = elemRef.page;
                index = elemRef.childIndex;
            } else {
                return null;
            }
        }
    }


    private void addToStack(final TraversablePage page, final int childIndex) {
        if (stackSize == stack.length) {
            stack = Arrays.copyOf(stack, stack.length << 1);
        }

        var ref = stack[stackSize];
        if (ref == null) {
            ref = new ElemRef(page, childIndex);
            stack[stackSize] = ref;
        } else {
            ref.page = page;
            ref.childIndex = childIndex;
        }

        stackSize++;
    }

    public void set(int itemIndex, final TraversablePage page, final int childIndex) {
        var ref = stack[itemIndex];
        if (ref == null) {
            ref = new ElemRef(page, childIndex);
            stack[itemIndex] = ref;
        } else {
            ref.page = page;
            ref.childIndex = childIndex;
        }
    }

    public void setItemAndSize(int itemIndex, final TraversablePage page, final int pageIndex) {
        if (itemIndex >= stack.length) {
            var stackCapacity = MathUtil.closestPowerOfTwo(itemIndex + 1);
            stack = Arrays.copyOf(stack, stackCapacity);
        }

        var newSize = itemIndex + 1;
        assert stackSize <= newSize;

        stackSize = newSize;
        set(itemIndex, page, pageIndex);
    }

    public TraversablePage getOrClear(int depth, int childIndex) {
        if (depth >= stackSize - 1) {
            return null;
        }

        var ref = stack[depth];
        if (ref.childIndex == childIndex) {
            return ref.page;
        }

        ref.childIndex = -1;
        ref.page = null;
        stackSize = depth + 1;

        return null;
    }

    public void clear(int depth) {
        if (depth > stackSize - 1) {
            return;
        }

        var ref = stack[depth];

        ref.childIndex = -1;
        ref.page = null;
        stackSize = depth + 1;
    }

    public boolean isEmpty() {
        return stackSize == 0;
    }

    public boolean isFull() {
        return stackSize > 0;
    }

    private void removeFromStack() {
        if (stackSize == 0) {
            throw new IllegalStateException();
        }

        stackSize--;
        var ref = stack[stackSize];

        ref.page = null;
        ref.childIndex = -1;
    }

    enum Traverse {
        LAST,
        FIRST,
        NONE
    }
}
