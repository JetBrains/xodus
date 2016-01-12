/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.core.dataStructures.skiplists;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unchecked")
public class SkipList<K extends Comparable<? super K>> extends SkipListBase {

    public abstract static class SkipListNode<K extends Comparable<? super K>> {

        @NotNull
        private final K key;
        private SkipListNode<K> prev;
        private SkipListNode<K> next;

        protected SkipListNode(@NotNull final K key) {
            this.key = key;
        }

        @NotNull
        public K getKey() {
            return key;
        }

        protected abstract int getLevels();

        protected abstract SkipListNode<K> getNext(int level);

        protected abstract void setNext(final SkipListNode<K> next, int level);

        protected final SkipListNode<K> getPrev() {
            return prev;
        }

        protected final void setPrev(final SkipListNode<K> prev) {
            this.prev = prev;
        }

        protected final SkipListNode<K> getNext() {
            return next;
        }

        protected final void setNext(@Nullable final SkipListNode<K> next) {
            this.next = next;
        }
    }

    @NotNull
    private final SkipListInternalNode<K> root;
    private SkipListNode<K>[] prevList;

    public SkipList() {
        root = new SkipListInternalNode("just a fake key which very unlikely would ever be used in real life ;-)");
        clear();
    }

    public void clear() {
        root.setNext(null);
        root.setNexts(newNodeArray(3));
        prevList = new SkipListNode[]{root, root, root, root};
        size = 0;
    }

    public void add(@NotNull final K key) {

        // 1. search for the right place for the key within lists saving previous nodes in the prevs array.
        SkipListNode<K> next = root;
        int level = next.getLevels() - 1;
        final SkipListNode<K>[] prevs = prevList;
        SkipListNode<K> temp;
        while (level > 0) {
            while ((temp = next.getNext(level)) != null && compare(key, temp.getKey()) >= 0) {
                next = temp;
            }
            prevs[level--] = next;
        }
        while ((temp = next.getNext()) != null && compare(key, temp.getKey()) >= 0) {
            next = temp;
        }
        prevs[0] = next;

        final SkipListNode<K> prev = prevs[0];
        level = prev != root && prev.getLevels() > 1 ? 1 : generateQuasiRandomLevel();

        // 2. perform insertion
        next = prev.getNext();
        final SkipListNode<K> newNode;
        switch (level) {
            case 1: {
                newNode = new SkipListBottomNode<>(key);
                break;
            }
            case 2: {
                newNode = new SkipListLevel2Node<>(key);
                final SkipListNode<K> prev1 = prevs[1];
                newNode.setNext(prev1.getNext(1), 1);
                prev1.setNext(newNode, 1);
                break;
            }
            case 3: {
                newNode = new SkipListLevel3Node<>(key);
                final SkipListNode<K> prev1 = prevs[1];
                newNode.setNext(prev1.getNext(1), 1);
                prev1.setNext(newNode, 1);
                final SkipListNode<K> prev2 = prevs[2];
                newNode.setNext(prev2.getNext(2), 2);
                prev2.setNext(newNode, 2);
                break;
            }
            default: {
                final SkipListInternalNode<K> internalNode = new SkipListInternalNode<>(key);
                newNode = internalNode;
                final int rootLevels = root.getLevels();
                final SkipListNode<K>[] nexts = newNodeArray(level - 1);
                for (int i = 1; i < level && i < rootLevels; ++i) {
                    nexts[i - 1] = prevs[i].getNext(i);
                    prevs[i].setNext(newNode, i);
                }
                internalNode.setNexts(nexts);
                break;
            }
        }
        newNode.setNext(next);
        newNode.setPrev(prev);
        prev.setNext(newNode);
        if (next != null) {
            next.setPrev(newNode);
        }

        // 3. adjust root list if newly added node is present in more layers than the root.
        adjustRootNode(newNode, level);

        ++size;
    }

    @Nullable
    public K getMinimum() {
        final SkipListNode<K> node = getMinimumNode();
        return node == null ? null : node.getKey();
    }

    @NotNull
    public SkipListNode<K> getFakeRoot() {
        return root;
    }

    @Nullable
    public SkipListNode<K> getMinimumNode() {
        return size == 0 ? null : root.getNext();
    }

    @Nullable
    public K getMaximum() {
        final SkipListNode<K> node = getMaximumNode();
        return node == null ? null : node.getKey();
    }

    @Nullable
    public SkipListNode<K> getMaximumNode() {
        SkipListNode<K> result = null;
        SkipListNode<K> next = root;
        int level = next.getLevels() - 1;
        while (level >= 0) {
            SkipListNode<K> temp;
            while ((temp = next.getNext(level)) != null) {
                next = temp;
            }
            result = next;
            --level;
        }
        return (result == root) ? null : result;
    }

    public SkipListNode<K> getNext(final SkipListNode<K> node) {
        return node.getNext();
    }

    public SkipListNode<K> getPrevious(final SkipListNode<K> node) {
        final SkipListNode<K> prev = node.getPrev();
        return (prev == root) ? null : prev;
    }

    @Nullable
    public SkipListNode<K> search(@NotNull final K key) {
        SkipListNode<K> next = root;
        int level = next.getLevels() - 1;
        SkipListNode<K> temp;
        int cmp;
        while (level > 0) {
            cmp = -1;
            while ((temp = next.getNext(level)) != null && (cmp = compare(key, temp.getKey())) > 0) {
                next = temp;
            }
            if (cmp == 0) return temp;
            --level;
        }
        cmp = -1;
        while ((temp = next.getNext()) != null && (cmp = compare(key, temp.getKey())) > 0) {
            next = temp;
        }
        return (cmp == 0) ? temp : null;
    }

    public boolean remove(@NotNull final K key) {
        SkipListNode<K> next = root;
        int level = next.getLevels() - 1;
        final SkipListNode<K>[] prevs = prevList;
        SkipListNode<K> foundNode = null;
        while (level > 0) {
            for (; ; ) {
                prevs[level] = next;
                final SkipListNode<K> temp;
                int cmp = -1;
                if ((temp = next.getNext(level)) == null || (cmp = compare(key, temp.getKey())) <= 0) {
                    if (cmp == 0) {
                        foundNode = temp;
                    }
                    break;
                }
                next = temp;
            }
            --level;
        }
        for (; ; ) {
            prevs[0] = next;
            final SkipListNode<K> temp;
            int cmp = -1;
            if ((temp = next.getNext()) == null || (cmp = compare(key, temp.getKey())) <= 0) {
                if (cmp == 0) {
                    foundNode = temp;
                }
                break;
            }
            next = temp;
        }
        if (foundNode != null) {
            level = foundNode.getLevels();
            for (int i = 1; i < level; ++i) {
                prevs[i].setNext(foundNode.getNext(i), i);
            }
            next = foundNode.getNext();
            final SkipListNode<K> prev = prevs[0];
            prev.setNext(next);
            if (next != null) {
                next.setPrev(prev);
            }
            if (--size == 0) {
                clear();
            }
            return true;
        }
        return false;
    }

    @Nullable
    public SkipListNode<K> getGreaterOrEqual(@NotNull final K key) {
        SkipListNode<K> next = root;
        int level = next.getLevels() - 1;
        SkipListNode<K> temp;
        int cmp;
        while (level > 0) {
            cmp = -1;
            while ((temp = next.getNext(level)) != null && (cmp = compare(key, temp.getKey())) > 0) {
                next = temp;
            }
            if (cmp == 0) {
                return getMostLeftEqualNode(temp);
            }
            --level;
        }
        cmp = -1;
        while ((temp = next.getNext()) != null && (cmp = compare(key, temp.getKey())) > 0) {
            next = temp;
        }
        if (cmp == 0) {
            return getMostLeftEqualNode(temp);
        }
        return next.getNext();
    }

    @Nullable
    public SkipListNode<K> getLessOrEqual(@NotNull final K key) {
        SkipListNode<K> next = root;
        int level = next.getLevels() - 1;
        SkipListNode<K> temp;
        int cmp;
        while (level >= 0) {
            cmp = -1;
            while ((temp = next.getNext(level)) != null && (cmp = compare(key, temp.getKey())) > 0) {
                next = temp;
            }
            if (cmp == 0) {
                return getMostLeftEqualNode(temp);
            }
            --level;
        }
        cmp = -1;
        while ((temp = next.getNext()) != null && (cmp = compare(key, temp.getKey())) > 0) {
            next = temp;
        }
        if (cmp == 0) {
            return getMostLeftEqualNode(temp);
        }
        return (next == root) ? null : next;
    }

    /**
     * Perform necessary changes when a node of maximum level appears.
     *
     * @param node  - the node.
     * @param level - its level.
     */
    private void adjustRootNode(final SkipListNode<K> node, final int level) {
        final SkipListInternalNode<K> root = this.root;
        final int rootLevels = root.getLevels();
        if (rootLevels < level) {
            prevList = newNodeArray(level);
            final SkipListNode<K>[] newRootNexts = newNodeArray(level - 1);
            int i = 1;
            for (; i < rootLevels; ++i) {
                newRootNexts[i - 1] = root.getNext(i);
            }
            for (; i < newRootNexts.length + 1; ++i) {
                newRootNexts[i - 1] = node;
            }
            root.setNexts(newRootNexts);
        }
    }

    /**
     * @param node which is equal to the result.
     * @return the most left (first in the list's order) node equal to the given one.
     */
    private SkipListNode<K> getMostLeftEqualNode(final SkipListNode<K> node) {
        SkipListNode<K> result;
        SkipListNode<K> prev = node;
        final K key = node.getKey();
        do {
            result = prev;
        } while ((prev = prev.getPrev()) != root && compare(prev.getKey(), key) == 0);
        return result;
    }

    private int compare(@NotNull final K k1, @NotNull final K k2) {
        return k1.compareTo(k2);
    }

    private SkipListNode<K>[] newNodeArray(int size) {
        return new SkipListNode[size];
    }

    /**
     * A node that is present at several layers of skip-list.
     */
    private static final class SkipListInternalNode<K extends Comparable<? super K>> extends SkipListNode<K> {

        private SkipListNode<K>[] _nexts;

        private SkipListInternalNode(@NotNull final K key) {
            super(key);
        }

        @Override
        protected int getLevels() {
            return _nexts.length + 1;
        }

        @Override
        protected SkipListNode<K> getNext(int level) {
            return (level == 0) ? getNext() : _nexts[level - 1];
        }

        @Override
        protected void setNext(final SkipListNode<K> next, int level) {
            if (level == 0) {
                setNext(next);
            } else {
                _nexts[level - 1] = next;
            }
        }

        private void setNexts(final SkipListNode<K>[] nexts) {
            _nexts = nexts;
        }
    }

    /**
     * Bottom node with the only 'next' reference (is present at the bottom layer only).
     */
    private static final class SkipListBottomNode<K extends Comparable<? super K>> extends SkipListNode<K> {


        private SkipListBottomNode(@NotNull final K key) {
            super(key);
        }

        @Override
        protected int getLevels() {
            return 1;
        }

        @Override
        protected SkipListNode<K> getNext(int level) {
            checkLevel(level);
            return getNext();
        }

        @Override
        protected void setNext(final SkipListNode<K> next, int level) {
            checkLevel(level);
            setNext(next);
        }

        private void checkLevel(int level) {
            if (level != 0) {
                throw new IllegalArgumentException("SkipListBottomNode: level can only be equal to 0.");
            }
        }
    }

    private static final class SkipListLevel2Node<K extends Comparable<? super K>> extends SkipListNode<K> {

        private SkipListNode<K> _next2; // level 2 reference

        private SkipListLevel2Node(K key) {
            super(key);
        }

        @Override
        protected int getLevels() {
            return 2;
        }

        @Override
        protected SkipListNode<K> getNext(int level) {
            if (level == 1) return _next2;
            if (level == 0) return getNext();
            throwBadLevel();
            return null;
        }

        @Override
        protected void setNext(final SkipListNode<K> next, int level) {
            if (level == 1) {
                _next2 = next;
            } else if (level == 0) {
                setNext(next);
            } else {
                throwBadLevel();
            }
        }

        private static void throwBadLevel() {
            throw new IllegalArgumentException("SkipListLevel2Node: level can only be equal to 0 or 1.");
        }
    }

    private static final class SkipListLevel3Node<K extends Comparable<? super K>> extends SkipListNode<K> {

        private SkipListNode<K> _next2; // level 2 reference
        private SkipListNode<K> _next3; // level 3 reference

        private SkipListLevel3Node(K key) {
            super(key);
        }

        @Override
        protected int getLevels() {
            return 3;
        }

        @Override
        protected SkipListNode<K> getNext(int level) {
            switch (level) {
                case 2:
                    return _next3;
                case 1:
                    return _next2;
                case 0:
                    return getNext();
                default:
                    throwBadLevel();
            }
            return null;
        }

        @Override
        protected void setNext(final SkipListNode<K> next, int level) {
            switch (level) {
                case 2:
                    _next3 = next;
                    break;
                case 1:
                    _next2 = next;
                    break;
                case 0:
                    setNext(next);
                    break;
                default:
                    throwBadLevel();
            }
        }

        private static void throwBadLevel() {
            throw new IllegalArgumentException("SkipListLevel3Node: level can only be equal to 0, 1 or 2.");
        }
    }
}
