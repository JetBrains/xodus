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
import jetbrains.exodus.tree.INode;
import jetbrains.exodus.tree.MutableTreeRoot;
import jetbrains.exodus.tree.TreeTraverser;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.NoSuchElementException;

class BTreeTraverser implements TreeTraverser {
    @NotNull
    protected TreePos[] stack = new TreePos[8];

    protected int top = 0;

    protected BasePage currentNode;

    protected ILeafNode node = ILeafNode.EMPTY;

    protected int currentPos;

    BTreeTraverser(@NotNull BasePage currentNode) {
        this.currentNode = currentNode;
    }

    // for tests only
    private BTreeTraverser(@NotNull BTreeTraverser source) {
        stack = source.stack; // tricky
        currentNode = source.currentNode;
        currentPos = source.currentPos;
    }

    @Override
    public void init(boolean left) {
        final int size = currentNode.size;
        currentPos = left ? 0 : size - 1;
        if (!canMoveDown()) {
            currentPos = left ? -1 : size;
        }
    }

    @Override
    public boolean isNotEmpty() {
        return currentNode.size > 0;
    }

    @Override
    @NotNull
    public ByteIterable getKey() {
        return node.getKey();
    }

    @Override
    @NotNull
    public ByteIterable getValue() {
        final ByteIterable result = node.getValue();
        if (result == null) {
            throw new NullPointerException();
        }
        return result;
    }

    @Override
    public boolean hasValue() {
        return node.hasValue();
    }

    @Override
    @NotNull
    public INode moveDown() {
        return node = pushChild(new TreePos(currentNode, currentPos), getChildForMoveDown(), 0);
    }

    @Override
    @NotNull
    public INode moveDownToLast() {
        final BasePage child = getChildForMoveDown();
        return node = pushChild(new TreePos(currentNode, currentPos), child, child.size - 1);
    }

    protected BasePage getChildForMoveDown() {
        return currentNode.getChild(currentPos);
    }

    protected ILeafNode pushChild(@NotNull final TreePos topPos, @NotNull final BasePage child, int pos) {
        setAt(top, topPos);
        currentNode = child;
        currentPos = pos;
        ++top;
        if (child.isBottom()) {
            return handleLeaf(child.getKey(pos));
        } else {
            return ILeafNode.EMPTY;
        }
    }

    protected ILeafNode handleLeaf(BaseLeafNode leaf) {
        return leaf;
    }

    protected void setAt(final int pos, @NotNull final TreePos treePos) {
        final int length = stack.length;
        if (pos >= length) { // ensure capacity
            final int newCapacity = length << 1;
            TreePos[] newStack = new TreePos[newCapacity];
            System.arraycopy(stack, 0, newStack, 0, length);
            stack = newStack;
        }
        stack[pos] = treePos;
    }

    @Override
    public void moveUp() {
        --top;
        final TreePos topPos = stack[top];
        currentNode = topPos.node;
        currentPos = topPos.pos;
        node = ILeafNode.EMPTY;
        stack[top] = null; // help gc
    }

    int getNextSibling(ByteIterable key) {
        return currentNode.binarySearch(key, currentPos);
    }

    @Override
    public int compareCurrent(@NotNull final ByteIterable key) {
        return currentNode.getKey(currentPos).compareKeyTo(key);
    }

    public void moveTo(int index) {
        currentPos = index;
    }

    public boolean canMoveTo(int index) {
        return index < currentNode.size;
    }

    @Override
    public boolean canMoveRight() {
        return currentPos + 1 < currentNode.size;
    }

    @Override
    @NotNull
    public INode moveRight() {
        ++currentPos;
        if (currentNode.isBottom()) {
            return node = handleLeafR(currentNode.getKey(currentPos));
        } else {
            return node = ILeafNode.EMPTY;
        }
    }

    protected ILeafNode handleLeafR(BaseLeafNode leaf) {
        return leaf;
    }

    protected ILeafNode handleLeafL(BaseLeafNode leaf) {
        return leaf;
    }

    @Override
    public boolean canMoveLeft() {
        return currentPos > 0;
    }

    @Override
    @NotNull
    public INode moveLeft() {
        --currentPos;
        if (currentNode.isBottom()) {
            return node = handleLeafL(currentNode.getKey(currentPos));
        } else {
            return node = ILeafNode.EMPTY;
        }
    }

    @Override
    public long getCurrentAddress() {
        return currentNode.getChildAddress(currentPos);
    }

    @Override
    public boolean canMoveUp() {
        return top != 0;
    }

    @Override
    public boolean canMoveDown() {
        return !currentNode.isBottom();
    }

    @Override
    public void reset(@NotNull MutableTreeRoot root) {
        top = 0;
        node = ILeafNode.EMPTY;
        currentNode = (BasePage) root;
        currentPos = 0;
    }

    @Override
    public boolean moveTo(ByteIterable key, @Nullable ByteIterable value) {
        return doMoveTo(key, value, false);
    }

    @Override
    public boolean moveToRange(ByteIterable key, @Nullable ByteIterable value) {
        return doMoveTo(key, value, true);
    }

    private boolean doMoveTo(@NotNull ByteIterable key, @Nullable ByteIterable value, boolean rangeSearch) {
        BasePage bottomNode = top == 0 ? currentNode : stack[0].node; // the most bottom node, ignoring lower bound
        final ILeafNode result = bottomNode.find(this, 0, key, value, rangeSearch);
        if (result == null) {
            return false;
        }
        node = result.isDupLeaf() ? new LeafNodeKV(result.getValue(), result.getKey()) : result;
        return true;
    }

    @NotNull
    @Override
    public BTreeBase getTree() {
        return (top == 0 ? currentNode : stack[0].node).getTree();
    }

    protected boolean isDup() {
        return false;
    }

    PageIterator iterator() { // for testing purposes
        return new PageIterator() {
            int index = 0;
            int currentIteratorPos = 0;
            BasePage currentIteratorNode = null;

            @Override
            public int getPos() {
                return currentIteratorPos;
            }

            @Override
            public boolean hasNext() {
                return index <= top; // equality means we should return current
            }

            @Override
            public BasePage next() {
                final BasePage next;
                if (index < top) {
                    final TreePos treePos = stack[index];
                    next = treePos.node;
                    currentIteratorPos = treePos.pos;
                } else {
                    if (index > top) {
                        throw new NoSuchElementException("No more pages in stack");
                    } else {
                        next = currentNode;
                        currentIteratorPos = currentPos;
                    }
                }
                currentIteratorNode = next;
                index++;
                return next;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    // for tests only
    static boolean isInDupMode(@NotNull final AddressIterator addressIterator) {
        // hasNext() updates 'inDupTree'
        return addressIterator.hasNext() && ((BTreeTraverserDup) addressIterator.getTraverser()).inDupTree;
    }

    // for tests only
    static BTreeTraverser getTraverserNoDup(@NotNull final AddressIterator addressIterator) {
        return new BTreeTraverser((BTreeTraverser) addressIterator.getTraverser());
    }

    interface PageIterator extends Iterator<BasePage> {
        int getPos();
    }
}
