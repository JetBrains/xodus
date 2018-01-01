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
import jetbrains.exodus.ByteIterableBase;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.tree.INode;
import jetbrains.exodus.tree.MutableTreeRoot;
import jetbrains.exodus.tree.TreeTraverser;
import jetbrains.exodus.util.LightOutputStream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class PatriciaTraverser implements TreeTraverser {

    private static final int INITIAL_STACK_CAPACITY = 8;

    @NotNull
    private final PatriciaTreeBase tree;
    @NotNull
    NodeChildrenIterator[] stack;
    int top;
    @NotNull
    NodeBase currentNode;
    @Nullable
    private ByteIterable currentValue;
    ChildReference currentChild;
    @Nullable
    NodeChildrenIterator currentIterator;

    PatriciaTraverser(@NotNull final PatriciaTreeBase tree, @NotNull final NodeBase currentNode) {
        this.tree = tree;
        setCurrentNode(currentNode);
        stack = new NodeChildrenIterator[INITIAL_STACK_CAPACITY];
        top = 0;
    }

    @Override
    public void init(boolean left) {
        if (left) {
            getItr();
        } else {
            currentIterator = currentNode.getChildrenLast();
            currentChild = currentIterator.getNode();
        }
    }

    @Override
    public boolean isNotEmpty() {
        return currentNode.getChildrenCount() > 0;
    }

    @Override
    @NotNull
    public INode moveDown() {
        stack = pushIterator(stack, currentIterator, top);
        setCurrentNode(currentChild.getNode(tree));
        getItr();
        ++top;
        return currentNode;
    }

    @Override
    @NotNull
    public INode moveDownToLast() {
        stack = pushIterator(stack, currentIterator, top);
        setCurrentNode(currentChild.getNode(tree));
        if (currentNode.getChildrenCount() > 0) {
            final NodeChildrenIterator itr = currentNode.getChildrenLast();
            currentIterator = itr;
            currentChild = itr.getNode();
        } else {
            currentIterator = null;
            currentChild = null;
        }
        ++top;
        return currentNode;
    }

    @NotNull
    @Override
    public ByteIterable getKey() {
        if (top == 0) {
            return currentNode.hasValue() ? currentNode.keySequence : ByteIterable.EMPTY;
        }
        final LightOutputStream output = new LightOutputStream();
        for (int i = 0; i < top; ++i) {
            ByteIterableBase.fillBytes(stack[i].getKey(), output);
            output.write(stack[i].getNode().firstByte); // seems that firstByte isn't mutated
        }
        ByteIterableBase.fillBytes(currentNode.keySequence, output);
        return output.asArrayByteIterable();
    }

    @NotNull
    @Override
    public ByteIterable getValue() {
        final ByteIterable result = currentValue;
        if (result == null) {
            return ByteIterable.EMPTY;
        }
        return result;
    }

    @Override
    public boolean hasValue() {
        return currentValue != null;
    }

    @Override
    public void moveUp() {
        --top;
        final NodeChildrenIterator topItr = stack[top];
        setCurrentNode(topItr.getParentNode());
        currentIterator = topItr;
        currentChild = topItr.getNode();
        stack[top] = null; // help gc
    }

    @Override
    public int compareCurrent(@NotNull ByteIterable key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean canMoveRight() {
        return currentIterator != null && currentIterator.hasNext();
    }

    boolean isValidPos() {
        return currentIterator != null;
    }

    @Override
    @NotNull
    public INode moveRight() {
        if (currentIterator.hasNext()) {
            if (currentIterator.isMutable()) {
                currentChild = currentIterator.next();
            } else {
                currentIterator.nextInPlace();
            }
        } else {
            currentIterator = null;
            currentChild = null;
        }
        return currentNode;
    }

    @Override
    public boolean canMoveLeft() {
        if (currentIterator == null) {
            return currentNode.getChildrenCount() > 0;
        }
        return currentIterator.hasPrev();
    }

    @Override
    @NotNull
    public INode moveLeft() {
        if (currentIterator.hasPrev()) {
            if (currentIterator.isMutable() || currentChild == null) {
                currentChild = currentIterator.prev();
            } else {
                currentIterator.prevInPlace();
            }
            return currentNode;
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public long getCurrentAddress() {
        return currentNode.getAddress();
    }

    @Override
    public boolean canMoveUp() {
        return top != 0;
    }

    @Override
    public boolean canMoveDown() {
        return currentChild != null;
    }

    @Override
    public void reset(@NotNull MutableTreeRoot root) {
        top = 0;
        setCurrentNode((NodeBase) root);
        getItr();
    }

    protected void setCurrentNode(@NotNull final NodeBase node) {
        currentNode = node;
        currentValue = node.getValue();
    }

    @Override
    public boolean moveTo(@NotNull ByteIterable key, @Nullable ByteIterable value) {
        final ByteIterator it = key.iterator();
        NodeBase node = top == 0 ? currentNode : stack[0].getParentNode(); // the most bottom node, ignoring lower bound
        int depth = 0;
        NodeChildrenIterator[] tmp = new NodeChildrenIterator[INITIAL_STACK_CAPACITY];
        // go down and search
        while (true) {
            if (NodeBase.MatchResult.getMatchingLength(node.matchesKeySequence(it)) < 0) {
                return false;
            }
            if (!it.hasNext()) {
                break;
            }
            final NodeChildrenIterator itr = node.getChildren(it.next());
            final ChildReference ref = itr.getNode();
            if (ref == null) {
                return false;
            }
            tmp = pushIterator(tmp, itr, depth++);
            node = ref.getNode(tree);
        }
        // key match
        if (node.hasValue() && (value == null || value.compareTo(node.getValue()) == 0)) {
            setCurrentNode(node);
            getItr();
            stack = tmp;
            top = depth;
            return true;
        }
        return false;
    }

    @Override
    public boolean moveToRange(@NotNull ByteIterable key, @Nullable ByteIterable value) {
        final ByteIterator it = key.iterator();
        NodeBase node = top == 0 ? currentNode : stack[0].getParentNode(); // the most bottom node, ignoring lower bound
        int depth = 0;
        NodeChildrenIterator[] tmp = new NodeChildrenIterator[INITIAL_STACK_CAPACITY];
        // go down and search
        final boolean dive;
        boolean smaller = false;
        while (true) {
            final boolean hasNext = it.hasNext();
            final long matchResult = node.matchesKeySequence(it);
            if (NodeBase.MatchResult.getMatchingLength(matchResult) < 0) {
                if (value == null) {
                    smaller = NodeBase.MatchResult.hasNext(matchResult) && (!hasNext ||
                        NodeBase.MatchResult.getKeyByte(matchResult) < NodeBase.MatchResult.getNextByte(matchResult));
                    dive = !smaller;
                    break;
                }
                return false;
            }
            if (!it.hasNext()) {
                // key match
                if (!node.hasValue()) {
                    dive = true;
                    break;
                }
                if (value == null || value.compareTo(node.getValue()) <= 0) {
                    setCurrentNode(node);
                    getItr();
                    stack = tmp;
                    top = depth;
                    return true;
                }
                return false;
            }
            final byte nextByte = it.next();
            NodeChildrenIterator itr = node.getChildren(nextByte);
            ChildReference ref = itr.getNode();
            if (ref == null) {
                itr = node.getChildrenRange(nextByte);
                ref = itr.getNode();
                if (ref != null) {
                    tmp = pushIterator(tmp, itr, depth++);
                    node = ref.getNode(tree);
                    dive = true;
                    break;
                }
                smaller = true;
                dive = false;
                break;
            }
            tmp = pushIterator(tmp, itr, depth++);
            node = ref.getNode(tree);
        }
        if (smaller || !node.hasValue()) {
            if (dive && node.getChildrenCount() > 0) {
                final NodeChildrenIterator itr = node.getChildren().iterator();
                tmp = pushIterator(tmp, itr, depth);
                node = itr.next().getNode(tree);
            } else {
                // go up and try range search
                NodeChildrenIterator itr;
                do {
                    if (depth > 0) {
                        itr = tmp[--depth];
                    } else {
                        return false; // search already gave us the max
                    }
                } while (!itr.hasNext());
                node = itr.next().getNode(tree);
                // trick: tmp[depth] was already in stack
            }
            ++depth;
            while (!node.hasValue()) {
                final NodeChildrenIterator itr = node.getChildren().iterator();
                if (!itr.hasNext()) {
                    throw new IllegalStateException("Can't dive into tree branch");
                }
                final ChildReference ref = itr.next();
                tmp = pushIterator(tmp, itr, depth++);
                node = ref.getNode(tree);
            }
        }
        setCurrentNode(node);
        getItr();
        stack = tmp;
        top = depth;
        return true;
    }

    @NotNull
    @Override
    public PatriciaTreeBase getTree() {
        return tree;
    }

    protected void getItr() {
        if (currentNode.getChildrenCount() > 0) {
            final NodeChildrenIterator itr = currentNode.getChildren().iterator();
            currentIterator = itr;
            currentChild = itr.next();
        } else {
            currentIterator = null;
            currentChild = null;
        }
    }

    private static NodeChildrenIterator[] pushIterator(NodeChildrenIterator[] tmp, NodeChildrenIterator itr, int depth) {
        final int length = tmp.length;
        if (depth >= length) { // ensure capacity
            final int newCapacity = length << 1;
            NodeChildrenIterator[] newStack = new NodeChildrenIterator[newCapacity];
            System.arraycopy(tmp, 0, newStack, 0, length);
            tmp = newStack;
        }
        tmp[depth] = itr;
        return tmp;
    }
}
