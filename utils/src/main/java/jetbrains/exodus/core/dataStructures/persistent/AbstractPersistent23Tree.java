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
package jetbrains.exodus.core.dataStructures.persistent;

import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.Stack;
import jetbrains.exodus.util.MathUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class AbstractPersistent23Tree<K extends Comparable<K>> implements Iterable<K> {

    abstract RootNode<K> getRoot();

    public boolean contains(K key) {
        Node<K> root = getRoot();
        return root != null && root.get(key) != null;
    }

    public final boolean isEmpty() {
        return getRoot() == null;
    }

    public final int size() {
        final RootNode<K> root = getRoot();
        return root == null ? 0 : root.getSize();
    }

    @Nullable
    public final K getMinimum() {
        final Iterator<K> it = iterator();
        return it.hasNext() ? it.next() : null;
    }

    @Nullable
    public final K getMaximum() {
        final Iterator<K> it = reverseIterator();
        return it.hasNext() ? it.next() : null;
    }

    private static class TreePos<K extends Comparable<K>> {

        Node<K> node;
        int pos;

        TreePos(Node<K> node) {
            this.node = node;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public final Iterator<K> iterator() {
        final RootNode<K> root = getRoot();
        if (root == null) {
            return Collections.EMPTY_LIST.iterator();
        }
        final TreePos<K>[] stack = new TreePos[MathUtil.integerLogarithm(root.getSize()) + 1];
        for (int i = 0; i < stack.length; ++i) {
            stack[i] = new TreePos<>(root);
        }
        return new Iterator<K>() {

            private int i = 0;
            private boolean hasNext;
            private boolean hasNextValid;

            @Override
            public boolean hasNext() {
                if (hasNextValid) {
                    return hasNext;
                }
                hasNextValid = true;
                TreePos<K> treePos = stack[i];
                if (treePos.node.isLeaf()) {
                    while (treePos.pos >= (treePos.node.isTernary() ? 2 : 1)) {
                        if (--i < 0) {
                            hasNext = false;
                            return hasNext;
                        }
                        treePos = stack[i];
                    }
                } else {
                    TreePos<K> newPos = stack[++i];
                    newPos.pos = 0;
                    if (treePos.pos == 0) {
                        newPos.node = treePos.node.getFirstChild();
                    } else if (treePos.pos == 1) {
                        newPos.node = treePos.node.getSecondChild();
                    } else {
                        newPos.node = treePos.node.getThirdChild();
                    }
                    treePos = newPos;
                    while (!treePos.node.isLeaf()) {
                        newPos = stack[++i];
                        newPos.pos = 0;
                        newPos.node = treePos.node.getFirstChild();
                        treePos = newPos;
                    }
                }
                treePos.pos++;
                return hasNext = true;
            }

            @Override
            public K next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                hasNextValid = false;
                final TreePos<K> treePos = stack[i];
                // treePos.pos must be 1 or 2 here
                return treePos.pos == 1 ? treePos.node.getFirstKey() : treePos.node.getSecondKey();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Returns an iterator that iterates over all elements greater or equal to key in ascending order
     *
     * @param key key
     * @return iterator
     */
    public Iterator<K> tailIterator(@NotNull final K key) {
        return new Iterator<K>() {

            private Stack<TreePos<K>> stack;
            private boolean hasNext;
            private boolean hasNextValid;

            @Override
            public boolean hasNext() {
                if (hasNextValid) {
                    return hasNext;
                }
                hasNextValid = true;
                if (stack == null) {
                    Node<K> root = getRoot();
                    if (root == null) {
                        hasNext = false;
                        return hasNext;
                    }
                    stack = new Stack<>();
                    if (!root.getLess(key, stack)) {
                        stack.push(new TreePos<>(root));
                    }
                }
                TreePos<K> treePos = stack.peek();
                if (treePos.node.isLeaf()) {
                    while (treePos.pos >= (treePos.node.isTernary() ? 2 : 1)) {
                        stack.pop();
                        if (stack.isEmpty()) {
                            hasNext = false;
                            return hasNext;
                        }
                        treePos = stack.peek();
                    }
                } else {
                    if (treePos.pos == 0) {
                        treePos = new TreePos<>(treePos.node.getFirstChild());
                    } else if (treePos.pos == 1) {
                        treePos = new TreePos<>(treePos.node.getSecondChild());
                    } else {
                        treePos = new TreePos<>(treePos.node.getThirdChild());
                    }
                    stack.push(treePos);
                    while (!treePos.node.isLeaf()) {
                        treePos = new TreePos<>(treePos.node.getFirstChild());
                        stack.push(treePos);
                    }
                }
                treePos.pos++;
                hasNext = true;
                return hasNext;
            }

            @Override
            public K next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                hasNextValid = false;
                TreePos<K> treePos = stack.peek();
                // treePos.pos must be 1 or 2 here
                return treePos.pos == 1 ? treePos.node.getFirstKey() : treePos.node.getSecondKey();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static class TreePosRev<K extends Comparable<K>> {

        private Node<K> node;
        private int pos;

        TreePosRev(Node<K> node) {
            setNode(node);
        }

        private void setNode(Node<K> node) {
            this.node = node;
            pos = 2;
            if (node.isTernary()) {
                pos = 3;
            }
        }
    }

    @SuppressWarnings("unchecked")
    public Iterator<K> reverseIterator() {
        if (isEmpty()) {
            return Collections.EMPTY_LIST.iterator();
        }
        final RootNode<K> root = getRoot();
        final TreePosRev<K>[] stack = new TreePosRev[MathUtil.integerLogarithm(root.getSize()) + 1];
        for (int i = 0; i < stack.length; ++i) {
            stack[i] = new TreePosRev<>(root);
        }
        return new Iterator<K>() {

            private int i = 0;
            private boolean hasNext;
            private boolean hasNextValid;

            @Override
            public boolean hasNext() {
                if (hasNextValid) {
                    return hasNext;
                }
                hasNextValid = true;
                TreePosRev<K> treePos = stack[i];
                if (treePos.node.isLeaf()) {
                    while (treePos.pos <= 1) {
                        if (--i < 0) {
                            hasNext = false;
                            return hasNext;
                        }
                        treePos = stack[i];
                    }
                } else {
                    TreePosRev<K> newPos = stack[++i];
                    if (treePos.pos == 1) {
                        newPos.setNode(treePos.node.getFirstChild());
                    } else if (treePos.pos == 2) {
                        newPos.setNode(treePos.node.getSecondChild());
                    } else {
                        newPos.setNode(treePos.node.getThirdChild());
                    }
                    treePos = newPos;
                    while (!treePos.node.isLeaf()) {
                        newPos = stack[++i];
                        newPos.setNode(treePos.node.isTernary() ? treePos.node.getThirdChild() : treePos.node.getSecondChild());
                        treePos = newPos;
                    }
                }
                treePos.pos--;
                return hasNext = true;
            }

            @Override
            public K next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                hasNextValid = false;
                final TreePosRev<K> treePos = stack[i];
                // treePos.pos must be 1 or 2 here
                return treePos.pos == 1 ? treePos.node.getFirstKey() : treePos.node.getSecondKey();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Returns an iterator that iterates over all elements greater or equal to key in descending order
     *
     * @param key key
     * @return iterator
     */
    public Iterator<K> tailReverseIterator(@NotNull final K key) {
        return new Iterator<K>() {

            private Stack<TreePosRev<K>> stack;
            private boolean hasNext;
            private boolean hasNextValid;
            private K bound;

            @Override
            public boolean hasNext() {
                if (hasNextValid) {
                    return hasNext;
                }
                hasNextValid = true;
                if (stack == null) {
                    Node<K> root = getRoot();
                    if (root == null) {
                        hasNext = false;
                        return hasNext;
                    }
                    bound = getLess(root, key);
                    stack = new Stack<>();
                    stack.push(new TreePosRev<>(root));
                }
                TreePosRev<K> treePos = stack.peek();
                if (treePos.node.isLeaf()) {
                    while (treePos.pos <= 1) {
                        stack.pop();
                        if (stack.isEmpty()) {
                            hasNext = false;
                            return hasNext;
                        }
                        treePos = stack.peek();
                    }
                } else {
                    if (treePos.pos == 1) {
                        treePos = new TreePosRev<>(treePos.node.getFirstChild());
                    } else if (treePos.pos == 2) {
                        treePos = new TreePosRev<>(treePos.node.getSecondChild());
                    } else {
                        treePos = new TreePosRev<>(treePos.node.getThirdChild());
                    }
                    stack.push(treePos);
                    while (!treePos.node.isLeaf()) {
                        treePos = new TreePosRev<>(treePos.node.isTernary() ? treePos.node.getThirdChild() : treePos.node.getSecondChild());
                        stack.push(treePos);
                    }
                }
                treePos.pos--;
                hasNext = tryNext() != bound;
                return hasNext;
            }

            @Override
            public K next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                hasNextValid = false;
                return tryNext();
            }

            private K tryNext() {
                TreePosRev<K> treePos = stack.peek();
                // treePos.pos must be 1 or 2 here
                return treePos.pos == 1 ? treePos.node.getFirstKey() : treePos.node.getSecondKey();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    static <K extends Comparable<K>> Node<K> createNode(@Nullable Node<K> firstChild, @NotNull K key, @Nullable Node<K> secondChild) {
        if (firstChild == null && secondChild == null) {
            return new BinaryNode<>(key);
        } else {
            return new InternalBinaryNode<>(firstChild, key, secondChild);
        }
    }

    static <K extends Comparable<K>> RootNode<K> createRootNode(@Nullable Node<K> firstChild, @NotNull K key,
                                                                @Nullable Node<K> secondChild, int size) {
        if (firstChild == null && secondChild == null) {
            return new RootBinaryNode<>(key, size);
        } else {
            return new RootInternalBinaryNode<>(firstChild, key, secondChild, size);
        }
    }

    static <K extends Comparable<K>> Node<K> createNode(@Nullable Node<K> firstChild, @NotNull K firstKey,
                                                        @Nullable Node<K> secondChild, @NotNull K secondKey,
                                                        @Nullable Node<K> thirdChild) {
        if (firstChild == null && secondChild == null && thirdChild == null) {
            return new TernaryNode<>(firstKey, secondKey);
        } else {
            return new InternalTernaryNode<>(firstChild, firstKey, secondChild, secondKey, thirdChild);
        }
    }

    static <K extends Comparable<K>> RootNode<K> createRootNode(@Nullable Node<K> firstChild, @NotNull K firstKey,
                                                                @Nullable Node<K> secondChild, @NotNull K secondKey,
                                                                @Nullable Node<K> thirdChild, int size) {
        if (firstChild == null && secondChild == null && thirdChild == null) {
            return new RootTernaryNode<>(firstKey, secondKey, size);
        } else {
            return new RootInternalTernaryNode<>(firstChild, firstKey, secondChild, secondKey, thirdChild, size);
        }
    }

    interface Node<K extends Comparable<K>> {
        Node<K> getFirstChild();

        Node<K> getSecondChild();

        Node<K> getThirdChild();

        K getFirstKey();

        K getSecondKey();

        boolean isLeaf();

        boolean isTernary();

        RootNode<K> asRoot(int size);

        /**
         * Inserts key into tree.
         * If tree already contains K k for which key.compareTo(k) == 0 tree *is* modified.
         * This is used by implementation of map using this set.
         *
         * @param key key to insert
         * @return result of insertion: either resulting node or two nodes with key to be added on the parent level
         */
        @NotNull
        SplitResult<K> insert(@NotNull K key);

        /**
         * @param key    key to remove
         * @param strict if strict is true removes key, else removes the first greater key
         * @return Replacing node of the new version of the tree and the removed key.
         * Node will be null if key's not found, replacing node of the resulting tree if no merge required,
         * node with single child and no key if merge is needed.
         * Key will be null if there's no such key.
         */
        Pair<Node<K>, K> remove(@NotNull K key, boolean strict);

        K get(@NotNull K key);

        K getByWeight(long weight);

        boolean getLess(K key, Stack<TreePos<K>> stack);

        /**
         * checks the subtree of this node
         *
         * @param from every key in this subtree must be at least from
         * @param to   every key in this subtree must be at most to
         * @return the depth of this subtree: must be the same for all the children
         */
        int checkNode(@Nullable K from, @Nullable K to);

        String print();

        void count(int[] count);
    }

    interface RootNode<K extends Comparable<K>> extends Node<K> {

        int getSize();
    }

    /**
     * checks the subtree of the node for correctness: for tests only
     *
     * @throws RuntimeException if the tree structure is incorrect
     */
    static <K extends Comparable<K>> void checkNode(Node<K> node) {
        node.checkNode(null, null);
    }

    static <K extends Comparable<K>> boolean getLess(Node<K> node, Stack<TreePos<K>> stack) {
        stack.push(new TreePos<>(node));
        return false;
    }

    static <K extends Comparable<K>> K getLess(Node<K> node, K key) {
        Stack<TreePos<K>> stack = new Stack<>();
        if (!node.getLess(key, stack)) {
            return null;
        }
        TreePos<K> treePos = stack.peek();
        return treePos.pos == 1 ? treePos.node.getFirstKey() : treePos.node.getSecondKey();
    }

    static class RootBinaryNode<K extends Comparable<K>> extends BinaryNode<K> implements RootNode<K> {

        private final int size;

        RootBinaryNode(@NotNull K firstKey, int size) {
            super(firstKey);
            this.size = size;
        }

        @Override
        public int getSize() {
            return size;
        }
    }

    static class BinaryNode<K extends Comparable<K>> implements Node<K> {

        private final K firstKey;

        BinaryNode(@NotNull K firstKey) {
            this.firstKey = firstKey;
        }

        @Override
        public Node<K> getFirstChild() {
            return null;
        }

        @Override
        public Node<K> getSecondChild() {
            return null;
        }

        @Override
        public Node<K> getThirdChild() {
            throw new UnsupportedOperationException();
        }

        @Override
        public K getFirstKey() {
            return firstKey;
        }

        @Override
        public K getSecondKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isLeaf() {
            return true;
        }

        @Override
        public boolean isTernary() {
            return false;
        }

        @Override
        public RootNode<K> asRoot(int size) {
            return new RootBinaryNode<>(firstKey, size);
        }

        @NotNull
        @Override
        public SplitResult<K> insert(@NotNull K key) {
            int comp = key.compareTo(firstKey);
            if (comp < 0) {
                return new SplitResult<K>().fill(new TernaryNode<>(key, firstKey)).setSizeChanged();
            }
            if (comp == 0) {
                return new SplitResult<K>().fill(new BinaryNode<>(key));
            }
            return new SplitResult<K>().fill(new TernaryNode<>(firstKey, key)).setSizeChanged();
        }

        @Override
        public Pair<Node<K>, K> remove(@NotNull K key, boolean strict) {
            int comp = strict ? key.compareTo(firstKey) : -1;
            if (strict && comp != 0) {
                return null;
            } else {
                return new Pair<Node<K>, K>(new RemovedNode<K>(null), firstKey);
            }
        }

        @Override
        public K get(@NotNull K key) {
            return key.compareTo(firstKey) == 0 ? firstKey : null;
        }

        @Override
        public K getByWeight(long weight) {
            final long firstKeyWeight = ((LongComparable) firstKey).getWeight();
            return firstKeyWeight == weight ? firstKey : null;
        }

        @Override
        public boolean getLess(K key, Stack<TreePos<K>> stack) {
            AbstractPersistent23Tree.getLess(this, stack);
            if (firstKey.compareTo(key) >= 0) {
                stack.pop();
                return false;
            }
            stack.peek().pos++;
            return true;
        }

        @Override
        public int checkNode(K from, K to) {
            if (from != null && from.compareTo(firstKey) >= 0) {
                throw new RuntimeException("Not a search tree.");
            }
            if (to != null && to.compareTo(firstKey) <= 0) {
                throw new RuntimeException("Not a search tree.");
            }
            return 1;
        }

        @Override
        public String print() {
            return String.valueOf(firstKey);
        }

        @Override
        public void count(int[] count) {
            count[0]++;
        }
    }

    static class RootInternalBinaryNode<K extends Comparable<K>> extends InternalBinaryNode<K> implements RootNode<K> {

        private final int size;

        RootInternalBinaryNode(@NotNull Node<K> firstChild, @NotNull K firstKey, @NotNull Node<K> secondChild, int size) {
            super(firstChild, firstKey, secondChild);
            this.size = size;
        }

        @Override
        public int getSize() {
            return size;
        }
    }

    static class InternalBinaryNode<K extends Comparable<K>> implements Node<K> {

        private final K firstKey;
        private final Node<K> firstChild;
        private final Node<K> secondChild;

        InternalBinaryNode(@NotNull Node<K> firstChild, @NotNull K firstKey, @NotNull Node<K> secondChild) {
            this.firstKey = firstKey;
            this.firstChild = firstChild;
            this.secondChild = secondChild;
        }

        @Override
        public Node<K> getFirstChild() {
            return firstChild;
        }

        @Override
        public Node<K> getSecondChild() {
            return secondChild;
        }

        @Override
        public Node<K> getThirdChild() {
            throw new UnsupportedOperationException();
        }

        @Override
        public K getFirstKey() {
            return firstKey;
        }

        @Override
        public K getSecondKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isLeaf() {
            return false;
        }

        @Override
        public boolean isTernary() {
            return false;
        }

        @Override
        public RootNode<K> asRoot(int size) {
            return new RootInternalBinaryNode<>(firstChild, firstKey, secondChild, size);
        }

        @NotNull
        @Override
        public SplitResult<K> insert(@NotNull K key) {
            int comp = key.compareTo(firstKey);
            if (comp < 0) {
                SplitResult<K> splitResult = firstChild.insert(key);
                final Node<K> firstNode = splitResult.getFirstNode();
                final K splitKey = splitResult.getKey();
                // no split
                if (splitKey == null) {
                    return splitResult.fill(new InternalBinaryNode<>(firstNode, firstKey, secondChild));
                }
                // split occurred
                return splitResult.fill(new InternalTernaryNode<>(firstNode, splitKey, splitResult.getSecondNode(), firstKey, secondChild));
            }
            if (comp == 0) {
                return new SplitResult<K>().fill(new InternalBinaryNode<>(firstChild, key, secondChild));
            }

            SplitResult<K> splitResult = secondChild.insert(key);
            final Node<K> firstNode = splitResult.getFirstNode();
            final K splitKey = splitResult.getKey();
            // no split
            if (splitKey == null) {
                return splitResult.fill(new InternalBinaryNode<>(firstChild, firstKey, firstNode));
            }
            // split occurred
            return splitResult.fill(new InternalTernaryNode<>(firstChild, firstKey, firstNode, splitKey, splitResult.getSecondNode()));
        }

        @SuppressWarnings({"SimplifiableConditionalExpression", "ConditionalExpressionWithNegatedCondition", "OverlyLongMethod"})
        @Override
        public Pair<Node<K>, K> remove(@NotNull K key, boolean strict) {
            int comp = strict ? key.compareTo(firstKey) : -1;
            if (comp < 0) {
                Pair<Node<K>, K> removeResult = firstChild.remove(key, strict);
                if (removeResult == null) {
                    return null;
                }
                Node<K> resultNode = removeResult.getFirst();
                K removedKey = removeResult.getSecond();
                if (resultNode instanceof RemovedNode) {
                    Node<K> removedNodeResult = resultNode.getFirstChild();
                    if (!secondChild.isTernary()) {
                        final Node<K> node = createNode(
                                removedNodeResult, firstKey, secondChild.getFirstChild(), secondChild.getFirstKey(), secondChild.getSecondChild()
                        );
                        return new Pair<Node<K>, K>(new RemovedNode<>(node), removedKey);
                    } else {
                        return new Pair<Node<K>, K>(new InternalBinaryNode<>(
                                createNode(removedNodeResult, firstKey, secondChild.getFirstChild()), secondChild.getFirstKey(),
                                createNode(secondChild.getSecondChild(), secondChild.getSecondKey(), secondChild.getThirdChild())), removedKey);
                    }
                } else {
                    return new Pair<>(createNode(resultNode, firstKey, secondChild), removedKey);
                }
            }
            // strict is true here
            Pair<Node<K>, K> removeResult = secondChild.remove(key, comp != 0);
            if (removeResult == null) {
                return null;
            }
            Node<K> resultNode = removeResult.getFirst();
            K removedKey = comp == 0 ? firstKey : removeResult.getSecond();
            K newNodeKey = comp != 0 ? firstKey : removeResult.getSecond();
            if (resultNode instanceof RemovedNode) {
                Node<K> removedNodeResult = resultNode.getFirstChild();
                if (!firstChild.isTernary()) {
                    return new Pair<Node<K>, K>(new RemovedNode<>(createNode(
                            firstChild.getFirstChild(), firstChild.getFirstKey(), firstChild.getSecondChild(), newNodeKey, removedNodeResult
                    )), removedKey);
                } else {
                    return new Pair<Node<K>, K>(new InternalBinaryNode<>(
                            createNode(firstChild.getFirstChild(), firstChild.getFirstKey(), firstChild.getSecondChild()),
                            firstChild.getSecondKey(), createNode(firstChild.getThirdChild(), newNodeKey, removedNodeResult)), removedKey);
                }
            } else {
                return new Pair<Node<K>, K>(new InternalBinaryNode<>(firstChild, newNodeKey, resultNode), removedKey);
            }
        }

        @Override
        public K get(@NotNull K key) {
            int comp = key.compareTo(firstKey);
            if (comp == 0) {
                return firstKey;
            }
            if (comp < 0) {
                return firstChild.get(key);
            } else {
                return secondChild.get(key);
            }
        }

        @Override
        public K getByWeight(long weight) {
            final long firstKeyWeight = ((LongComparable) firstKey).getWeight();
            if (firstKeyWeight == weight) {
                return firstKey;
            }
            if (weight < firstKeyWeight) {
                return firstChild.getByWeight(weight);
            } else {
                return secondChild.getByWeight(weight);
            }
        }

        @Override
        public boolean getLess(K key, Stack<TreePos<K>> stack) {
            AbstractPersistent23Tree.getLess(this, stack);
            int comp = firstKey.compareTo(key);
            if (comp < 0) {
                stack.peek().pos++;
                secondChild.getLess(key, stack);
            } else if (!firstChild.getLess(key, stack)) {
                stack.pop();
                return false;
            }
            return true;
        }

        @Override
        public int checkNode(K from, K to) {
            if (from != null && from.compareTo(firstKey) >= 0) {
                throw new RuntimeException("Not a search tree.");
            }
            if (to != null && to.compareTo(firstKey) <= 0) {
                throw new RuntimeException("Not a search tree.");
            }
            if (firstChild == null || secondChild == null) {
                throw new RuntimeException("Not an inner node.");
            }
            int firstDepth = firstChild.checkNode(from, firstKey);
            int secondDepth = secondChild.checkNode(firstKey, to);
            if (firstDepth != secondDepth) {
                throw new RuntimeException("Not balanced tree.");
            }
            return firstDepth + 1;
        }

        @Override
        public String print() {
            return '(' + firstChild.print() + ") " + firstKey + " (" + secondChild.print() + ')';
        }

        @Override
        public void count(int[] count) {
            count[1]++;
            firstChild.count(count);
            secondChild.count(count);
        }
    }

    static class RootTernaryNode<K extends Comparable<K>> extends TernaryNode<K> implements RootNode<K> {

        private final int size;

        RootTernaryNode(@NotNull K firstKey, @NotNull K secondKey, int size) {
            super(firstKey, secondKey);
            this.size = size;
        }

        @Override
        public int getSize() {
            return size;
        }
    }

    static class TernaryNode<K extends Comparable<K>> implements Node<K> {

        private final K firstKey;
        private final K secondKey;

        TernaryNode(@NotNull K firstKey, @NotNull K secondKey) {
            this.firstKey = firstKey;
            this.secondKey = secondKey;
        }

        @Override
        public Node<K> getFirstChild() {
            return null;
        }

        @Override
        public Node<K> getSecondChild() {
            return null;
        }

        @Override
        public Node<K> getThirdChild() {
            return null;
        }

        @Override
        public K getFirstKey() {
            return firstKey;
        }

        @Override
        public K getSecondKey() {
            return secondKey;
        }

        @Override
        public boolean isLeaf() {
            return true;
        }

        @Override
        public boolean isTernary() {
            return true;
        }

        @Override
        public RootNode<K> asRoot(int size) {
            return new RootTernaryNode<>(firstKey, secondKey, size);
        }

        @NotNull
        @Override
        public SplitResult<K> insert(@NotNull K key) {
            int comp = key.compareTo(firstKey);
            if (comp < 0) {
                return new SplitResult<K>().fill(new BinaryNode<>(key), firstKey, new BinaryNode<>(secondKey)).setSizeChanged();
            }
            if (comp == 0) {
                return new SplitResult<K>().fill(new TernaryNode<>(key, secondKey));
            }
            comp = key.compareTo(secondKey);
            if (comp < 0) {
                return new SplitResult<K>().fill(new BinaryNode<>(firstKey), key, new BinaryNode<>(secondKey)).setSizeChanged();
            }
            if (comp == 0) {
                return new SplitResult<K>().fill(new TernaryNode<>(firstKey, key));
            }
            return new SplitResult<K>().fill(new BinaryNode<>(firstKey), secondKey, new BinaryNode<>(key)).setSizeChanged();
        }

        @Override
        public Pair<Node<K>, K> remove(@NotNull K key, boolean strict) {
            int compFirst = strict ? key.compareTo(firstKey) : -1;
            if (compFirst < 0 && strict) {
                return null;
            }
            if (compFirst <= 0) {
                return new Pair<Node<K>, K>(new BinaryNode<>(secondKey), firstKey);
            }
            // strict is true here
            int compSecond = -1;
            if (compFirst > 0) {
                compSecond = key.compareTo(secondKey);
            }
            if (compSecond != 0) {
                return null;
            } else {
                return new Pair<Node<K>, K>(new BinaryNode<>(firstKey), secondKey);
            }
        }

        @Override
        public K get(@NotNull K key) {
            int comp = key.compareTo(firstKey);
            if (comp == 0) {
                return firstKey;
            }
            if (comp > 0 && key.compareTo(secondKey) == 0) {
                return secondKey;
            }
            return null;
        }

        @Override
        public K getByWeight(long weight) {
            long keyWeight = ((LongComparable) firstKey).getWeight();
            if (keyWeight == weight) {
                return firstKey;
            }
            if (weight > keyWeight && weight == ((LongComparable) secondKey).getWeight()) {
                return secondKey;
            }
            return null;
        }

        @Override
        public boolean getLess(K key, Stack<TreePos<K>> stack) {
            AbstractPersistent23Tree.getLess(this, stack);
            if (firstKey.compareTo(key) >= 0) {
                stack.pop();
                return false;
            }
            if (secondKey.compareTo(key) < 0) {
                stack.peek().pos += 2;
            } else {
                stack.peek().pos++;
            }
            return true;
        }

        @Override
        public int checkNode(K from, K to) {
            if (from != null && from.compareTo(firstKey) >= 0) {
                throw new RuntimeException("Not a search tree.");
            }
            if (firstKey.compareTo(secondKey) >= 0) {
                throw new RuntimeException("Not a search tree.");
            }
            if (to != null && to.compareTo(secondKey) <= 0) {
                throw new RuntimeException("Not a search tree.");
            }
            return 1;
        }

        @Override
        public String print() {
            return firstKey + ", " + secondKey;
        }

        @Override
        public void count(int[] count) {
            count[2]++;
        }
    }

    static class RootInternalTernaryNode<K extends Comparable<K>> extends InternalTernaryNode<K> implements RootNode<K> {

        private final int size;

        RootInternalTernaryNode(@NotNull Node<K> firstChild, @NotNull K firstKey,
                                @NotNull Node<K> secondChild, @NotNull K secondKey,
                                @NotNull Node<K> thirdChild, int size) {
            super(firstChild, firstKey, secondChild, secondKey, thirdChild);
            this.size = size;
        }

        @Override
        public int getSize() {
            return size;
        }
    }

    static class InternalTernaryNode<K extends Comparable<K>> implements Node<K> {

        private final K firstKey;
        private final K secondKey;
        private final Node<K> firstChild;
        private final Node<K> secondChild;
        private final Node<K> thirdChild;

        InternalTernaryNode(@NotNull Node<K> firstChild, @NotNull K firstKey, @NotNull Node<K> secondChild, @NotNull K secondKey, @NotNull Node<K> thirdChild) {
            this.firstKey = firstKey;
            this.secondKey = secondKey;
            this.firstChild = firstChild;
            this.secondChild = secondChild;
            this.thirdChild = thirdChild;
        }

        @Override
        public Node<K> getFirstChild() {
            return firstChild;
        }

        @Override
        public Node<K> getSecondChild() {
            return secondChild;
        }

        @Override
        public Node<K> getThirdChild() {
            return thirdChild;
        }

        @Override
        public K getFirstKey() {
            return firstKey;
        }

        @Override
        public K getSecondKey() {
            return secondKey;
        }

        @Override
        public boolean isLeaf() {
            return false;
        }

        @Override
        public boolean isTernary() {
            return true;
        }

        @Override
        public RootNode<K> asRoot(int size) {
            return new RootInternalTernaryNode<>(firstChild, firstKey, secondChild, secondKey, thirdChild, size);
        }

        @NotNull
        @Override
        public SplitResult<K> insert(@NotNull K key) {
            int comp = key.compareTo(firstKey);
            if (comp < 0) {
                SplitResult<K> splitResult = firstChild.insert(key);
                final Node<K> firstNode = splitResult.getFirstNode();
                final K splitKey = splitResult.getKey();
                // no split
                if (splitKey == null) {
                    return splitResult.fill(cloneReplacingChild(firstChild, firstNode));
                }
                // split occurred
                return splitResult.fill(new InternalBinaryNode<>(firstNode, splitKey, splitResult.getSecondNode()), firstKey, new InternalBinaryNode<>(secondChild, secondKey, thirdChild));
            }
            if (comp == 0) {
                return new SplitResult<K>().fill(new InternalTernaryNode<>(firstChild, key, secondChild, secondKey, thirdChild));
            }

            comp = key.compareTo(secondKey);
            if (comp < 0) {
                SplitResult<K> splitResult = secondChild.insert(key);
                final Node<K> firstNode = splitResult.getFirstNode();
                final K splitKey = splitResult.getKey();
                // no split
                if (splitKey == null) {
                    return splitResult.fill(cloneReplacingChild(secondChild, firstNode));
                }
                // split occurred
                return splitResult.fill(new InternalBinaryNode<>(firstChild, firstKey, firstNode), splitKey, new InternalBinaryNode<>(splitResult.getSecondNode(), secondKey, thirdChild));
            }
            if (comp == 0) {
                return new SplitResult<K>().fill(new InternalTernaryNode<>(firstChild, firstKey, secondChild, key, thirdChild));
            }

            // node.secondKey != null
            SplitResult<K> splitResult = thirdChild.insert(key);
            final Node<K> firstNode = splitResult.getFirstNode();
            final K splitKey = splitResult.getKey();
            // no split
            if (splitKey == null) {
                return splitResult.fill(cloneReplacingChild(thirdChild, firstNode));
            }
            // split occurred
            return splitResult.fill(new InternalBinaryNode<>(firstChild, firstKey, secondChild), secondKey, new InternalBinaryNode<>(firstNode, splitKey, splitResult.getSecondNode()));
        }

        @SuppressWarnings({"ConditionalExpressionWithNegatedCondition", "OverlyLongMethod"})
        @Override
        public Pair<Node<K>, K> remove(@NotNull K key, boolean strict) {
            int compFirst = strict ? key.compareTo(firstKey) : -1;
            if (compFirst < 0) {
                Pair<Node<K>, K> removeResult = firstChild.remove(key, strict);
                if (removeResult == null) {
                    return null;
                }
                Node<K> resultNode = removeResult.getFirst();
                K removedKey = removeResult.getSecond();
                if (resultNode instanceof RemovedNode) {
                    Node<K> removedNodeResult = resultNode.getFirstChild();
                    if (!secondChild.isTernary()) {
                        return new Pair<Node<K>, K>(new InternalBinaryNode<>(createNode(
                                removedNodeResult, firstKey, secondChild.getFirstChild(), secondChild.getFirstKey(), secondChild.getSecondChild()
                        ), secondKey, thirdChild), removedKey);
                    } else {
                        return new Pair<Node<K>, K>(new InternalTernaryNode<>(createNode(removedNodeResult, firstKey, secondChild.getFirstChild()),
                                secondChild.getFirstKey(), createNode(secondChild.getSecondChild(), secondChild.getSecondKey(), secondChild.getThirdChild()),
                                secondKey, thirdChild), removedKey);
                    }
                } else {
                    return new Pair<>(cloneReplacingChild(firstChild, resultNode), removedKey);
                }
            }
            // strict is true here
            int compSecond = -1;
            if (compFirst > 0) {
                compSecond = key.compareTo(secondKey);
            }
            if (compSecond < 0) {
                Pair<Node<K>, K> removeResult = secondChild.remove(key, compFirst != 0);
                if (removeResult == null) {
                    return null;
                }
                Node<K> resultNode = removeResult.getFirst();
                K removedKey = compFirst == 0 ? firstKey : removeResult.getSecond();
                K newNodeKey = compFirst != 0 ? firstKey : removeResult.getSecond();
                if (resultNode instanceof RemovedNode) {
                    Node<K> removedNodeResult = resultNode.getFirstChild();
                    if (!firstChild.isTernary()) {
                        return new Pair<Node<K>, K>(new InternalBinaryNode<>(createNode(
                                firstChild.getFirstChild(), firstChild.getFirstKey(), firstChild.getSecondChild(), newNodeKey, removedNodeResult
                        ), secondKey, thirdChild), removedKey);
                    } else {
                        return new Pair<Node<K>, K>(new InternalTernaryNode<>(
                                createNode(firstChild.getFirstChild(), firstChild.getFirstKey(), firstChild.getSecondChild()),
                                firstChild.getSecondKey(), createNode(firstChild.getThirdChild(), newNodeKey, removedNodeResult), secondKey, thirdChild), removedKey);
                    }
                } else {
                    return new Pair<Node<K>, K>(new InternalTernaryNode<>(firstChild, newNodeKey, resultNode, secondKey, thirdChild), removedKey);
                }
            }

            Pair<Node<K>, K> removeResult = thirdChild.remove(key, compSecond != 0);
            if (removeResult == null) {
                return null;
            }
            Node<K> resultNode = removeResult.getFirst();
            K removedKey = compSecond == 0 ? secondKey : removeResult.getSecond();
            K newNodeKey = compSecond != 0 ? secondKey : removeResult.getSecond();
            if (resultNode instanceof RemovedNode) {
                Node<K> removedNodeResult = resultNode.getFirstChild();
                if (!secondChild.isTernary()) {
                    return new Pair<Node<K>, K>(new InternalBinaryNode<>(firstChild, firstKey, createNode(
                            secondChild.getFirstChild(), secondChild.getFirstKey(), secondChild.getSecondChild(), newNodeKey, removedNodeResult
                    )), removedKey);
                } else {
                    return new Pair<Node<K>, K>(new InternalTernaryNode<>(firstChild, firstKey,
                            createNode(secondChild.getFirstChild(), secondChild.getFirstKey(), secondChild.getSecondChild()),
                            secondChild.getSecondKey(), createNode(secondChild.getThirdChild(), newNodeKey, removedNodeResult)), removedKey);
                }
            } else {
                return new Pair<Node<K>, K>(new InternalTernaryNode<>(firstChild, firstKey, secondChild, newNodeKey, resultNode), removedKey);
            }
        }

        @Override
        public K get(@NotNull K key) {
            int comp = key.compareTo(firstKey);
            if (comp < 0) {
                return firstChild.get(key);
            }
            if (comp == 0) {
                return firstKey;
            }
            comp = key.compareTo(secondKey);
            if (comp == 0) {
                return secondKey;
            } else {
                return comp < 0 ? secondChild.get(key) : thirdChild.get(key);
            }
        }

        @Override
        public K getByWeight(long weight) {
            long keyWeight = ((LongComparable) firstKey).getWeight();
            if (weight < keyWeight) {
                return firstChild.getByWeight(weight);
            }
            if (keyWeight == weight) {
                return firstKey;
            }
            keyWeight = ((LongComparable) secondKey).getWeight();
            if (keyWeight == weight) {
                return secondKey;
            } else {
                if (weight < keyWeight) {
                    return secondChild.getByWeight(weight);
                } else {
                    return thirdChild.getByWeight(weight);
                }
            }
        }

        @Override
        public boolean getLess(K key, Stack<TreePos<K>> stack) {
            AbstractPersistent23Tree.getLess(this, stack);
            int comp = secondKey.compareTo(key);
            if (comp < 0) {
                stack.peek().pos += 2;
                thirdChild.getLess(key, stack);
                return true;
            }
            comp = firstKey.compareTo(key);
            if (comp < 0) {
                stack.peek().pos++;
                secondChild.getLess(key, stack);
            } else if (!firstChild.getLess(key, stack)) {
                stack.pop();
                return false;
            }
            return true;
        }

        Node<K> cloneReplacingChild(@NotNull Node<K> oldChild, @NotNull Node<K> newChild) {
            return new InternalTernaryNode<>(firstChild == oldChild ? newChild : firstChild, firstKey,
                    secondChild == oldChild ? newChild : secondChild, secondKey,
                    thirdChild == oldChild ? newChild : thirdChild);
        }

        @Override
        public int checkNode(K from, K to) {
            if (from != null && from.compareTo(firstKey) >= 0) {
                throw new RuntimeException("Not a search tree.");
            }
            if (firstKey.compareTo(secondKey) >= 0) {
                throw new RuntimeException("Not a search tree.");
            }
            if (to != null && to.compareTo(secondKey) <= 0) {
                throw new RuntimeException("Not a search tree.");
            }
            if (firstChild == null || secondChild == null || thirdChild == null) {
                throw new RuntimeException("The node has not enough children.");
            }
            int depth = firstChild.checkNode(from, firstKey);
            if (depth != secondChild.checkNode(firstKey, secondKey) ||
                    depth != thirdChild.checkNode(secondKey, to)) {
                throw new RuntimeException("Not a balanced tree.");
            }
            return depth + 1;
        }

        @Override
        public String print() {
            return '(' + firstChild.print() + ") " + firstKey + " (" + secondChild.print() + ") " + secondKey + " (" + thirdChild.print() + ')';
        }

        @Override
        public void count(int[] count) {
            count[3]++;
            firstChild.count(count);
            secondChild.count(count);
            thirdChild.count(count);
        }
    }

    static class RemovedNode<K extends Comparable<K>> implements Node<K> {

        private final Node<K> child;

        RemovedNode(Node<K> child) {
            this.child = child;
        }

        @Override
        public Node<K> getFirstChild() {
            return child;
        }

        @Override
        public Node<K> getSecondChild() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Node<K> getThirdChild() {
            throw new UnsupportedOperationException();
        }

        @Override
        public K getFirstKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public K getSecondKey() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isLeaf() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isTernary() {
            throw new UnsupportedOperationException();
        }

        @Override
        public RootNode<K> asRoot(int size) {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public SplitResult<K> insert(@NotNull K key) {
            throw new UnsupportedOperationException("Can't insert into a temporary tree.");
        }

        @Override
        public Pair<Node<K>, K> remove(@NotNull K key, boolean strict) {
            throw new UnsupportedOperationException("Can't remove from a temporary tree.");
        }

        @Override
        public K get(@NotNull K key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public K getByWeight(long weight) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getLess(K key, Stack<TreePos<K>> stack) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int checkNode(@Nullable K from, @Nullable K to) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String print() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void count(int[] count) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Used for adding values. Can be in one of the following states:
     * 1) Only firstNode is not null.
     * Means there was no split.
     * 2) All fields are not null.
     * Means the key should be inserted into the parent with firstNode and secondNode children instead the old single child.
     */
    static class SplitResult<K extends Comparable<K>> {

        private Node<K> firstNode;
        private Node<K> secondNode;
        private K key;
        boolean sizeChanged = false;

        SplitResult<K> fill(@Nullable Node<K> first, @Nullable K key, @Nullable Node<K> second) {
            firstNode = first;
            this.key = key;
            secondNode = second;
            return this;
        }

        public SplitResult<K> fill(@NotNull Node<K> node) {
            return fill(node, null, null);
        }

        public Node<K> getFirstNode() {
            return firstNode;
        }

        public Node<K> getSecondNode() {
            return secondNode;
        }

        public K getKey() {
            return key;
        }

        public SplitResult<K> setSizeChanged() {
            sizeChanged = true;
            return this;
        }
    }
}
