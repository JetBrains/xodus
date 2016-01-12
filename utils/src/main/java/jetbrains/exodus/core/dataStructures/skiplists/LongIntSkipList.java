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

import org.jetbrains.annotations.Nullable;

public class LongIntSkipList extends SkipListBase {

    public abstract static class SkipListNode {

        private final long key;
        private int value;
        private SkipListNode prev;
        private SkipListNode next;

        protected SkipListNode(final long key, final int value) {
            this.key = key;
            this.value = value;
        }

        public long getKey() {
            return key;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        protected abstract int getLevels();

        protected abstract SkipListNode getNext(int level);

        protected abstract void setNext(final SkipListNode next, int level);

        protected final SkipListNode getPrev() {
            return prev;
        }

        protected final void setPrev(final SkipListNode prev) {
            this.prev = prev;
        }

        protected final SkipListNode getNext() {
            return next;
        }

        protected final void setNext(@Nullable final SkipListNode next) {
            this.next = next;
        }
    }

    private final SkipListInternalNode root;
    private SkipListNode[] prevList;

    public LongIntSkipList() {
        root = new SkipListInternalNode(Long.MIN_VALUE, Integer.MIN_VALUE);
        clear();
    }

    public void clear() {
        root.setNext(null);
        root.setNexts(new SkipListNode[]{null, null, null});
        prevList = new SkipListNode[]{root, root, root, root};
        size = 0;
    }

    public void add(final long key, final int value) {

        // 1. search for the right place for the key within lists saving previous nodes in the prevs array.
        SkipListNode next = root;
        int level = next.getLevels() - 1;
        final SkipListNode[] prevs = prevList;
        SkipListNode temp;
        while (level > 0) {
            while ((temp = next.getNext(level)) != null && key >= temp.getKey()) {
                next = temp;
            }
            prevs[level--] = next;
        }
        while ((temp = next.getNext()) != null && key >= temp.getKey()) {
            next = temp;
        }
        prevs[0] = next;

        final SkipListNode prev = prevs[0];
        level = prev != root && prev.getLevels() > 1 ? 1 : generateQuasiRandomLevel();

        // 2. perform insertion
        next = prev.getNext();
        final SkipListNode newNode;
        switch (level) {
            case 1: {
                newNode = new SkipListBottomNode(key, value);
                break;
            }
            case 2: {
                newNode = new SkipListLevel2Node(key, value);
                final SkipListNode prev1 = prevs[1];
                newNode.setNext(prev1.getNext(1), 1);
                prev1.setNext(newNode, 1);
                break;
            }
            case 3: {
                newNode = new SkipListLevel3Node(key, value);
                final SkipListNode prev1 = prevs[1];
                newNode.setNext(prev1.getNext(1), 1);
                prev1.setNext(newNode, 1);
                final SkipListNode prev2 = prevs[2];
                newNode.setNext(prev2.getNext(2), 2);
                prev2.setNext(newNode, 2);
                break;
            }
            default: {
                final SkipListInternalNode internalNode = new SkipListInternalNode(key, value);
                newNode = internalNode;
                final int rootLevels = root.getLevels();
                final SkipListNode[] nexts = new SkipListNode[level - 1];
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
    public Long getMinimum() {
        final SkipListNode node = getMinimumNode();
        return node == null ? null : node.getKey();
    }

    @Nullable
    public SkipListNode getMinimumNode() {
        return size == 0 ? null : root.getNext();
    }

    @Nullable
    public Long getMaximum() {
        final SkipListNode node = getMaximumNode();
        return node == null ? null : node.getKey();
    }

    @Nullable
    public SkipListNode getMaximumNode() {
        SkipListNode result = null;
        SkipListNode next = root;
        int level = next.getLevels() - 1;
        while (level >= 0) {
            SkipListNode temp;
            while ((temp = next.getNext(level)) != null) {
                next = temp;
            }
            result = next;
            --level;
        }
        return result == root ? null : result;
    }

    public SkipListNode getNext(final SkipListNode node) {
        return node.getNext();
    }

    public SkipListNode getPrevious(final SkipListNode node) {
        final SkipListNode prev = node.getPrev();
        return prev == root ? null : prev;
    }

    @Nullable
    public SkipListNode search(final long key) {
        SkipListNode next = root;
        int level = next.getLevels() - 1;
        SkipListNode temp;
        while (level > 0) {
            while ((temp = next.getNext(level)) != null) {
                final long tempKey = temp.getKey();
                if (key == tempKey) {
                    return temp;
                }
                if (key < tempKey) {
                    break;
                }
                next = temp;
            }
            --level;
        }
        while ((temp = next.getNext()) != null) {
            final long tempKey = temp.getKey();
            if (key == tempKey) {
                return temp;
            }
            if (key < tempKey) {
                break;
            }
            next = temp;
        }
        return null;
    }

    public boolean remove(final long key) {
        SkipListNode next = root;
        int level = next.getLevels() - 1;
        final SkipListNode[] prevs = prevList;
        SkipListNode foundNode = null;
        while (level > 0) {
            for (; ; ) {
                prevs[level] = next;
                final SkipListNode temp = next.getNext(level);
                if (temp == null) {
                    break;
                }
                final long tempKey = temp.getKey();
                if (key <= tempKey) {
                    if (key == tempKey) {
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
            final SkipListNode temp = next.getNext();
            if (temp == null) {
                break;
            }
            final long tempKey = temp.getKey();
            if (key <= tempKey) {
                if (key == tempKey) {
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
            final SkipListNode prev = prevs[0];
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
    public SkipListNode getGreaterOrEqual(final long key) {
        SkipListNode next = root;
        int level = next.getLevels() - 1;
        SkipListNode temp;
        while (level > 0) {
            while ((temp = next.getNext(level)) != null) {
                final long tempKey = temp.getKey();
                if (key == tempKey) {
                    return getMostLeftEqualNode(temp);
                }
                if (key < tempKey) {
                    break;
                }
                next = temp;
            }
            --level;
        }
        while ((temp = next.getNext()) != null) {
            final long tempKey = temp.getKey();
            if (key == tempKey) {
                return getMostLeftEqualNode(temp);
            }
            if (key < tempKey) {
                break;
            }
            next = temp;
        }
        return next.getNext();
    }

    @Nullable
    public SkipListNode getLessOrEqual(final long key) {
        SkipListNode next = root;
        int level = next.getLevels() - 1;
        SkipListNode temp;
        while (level >= 0) {
            while ((temp = next.getNext(level)) != null) {
                final long tempKey = temp.getKey();
                if (key == tempKey) {
                    return getMostLeftEqualNode(temp);
                }
                if (key < tempKey) {
                    break;
                }
                next = temp;
            }
            --level;
        }
        while ((temp = next.getNext()) != null) {
            final long tempKey = temp.getKey();
            if (key == tempKey) {
                return getMostLeftEqualNode(temp);
            }
            if (key < tempKey) {
                break;
            }
            next = temp;
        }
        return next == root ? null : next;
    }

    /**
     * Perform necessary changes when a node of maximum level appears.
     *
     * @param node  - the node.
     * @param level - its level.
     */
    private void adjustRootNode(final SkipListNode node, final int level) {
        final SkipListInternalNode root = this.root;
        final int rootLevels = root.getLevels();
        if (rootLevels < level) {
            prevList = new SkipListNode[level];
            final SkipListNode[] newRootNexts = new SkipListNode[level - 1];
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
    private SkipListNode getMostLeftEqualNode(final SkipListNode node) {
        SkipListNode result;
        SkipListNode prev = node;
        final long key = node.getKey();
        do {
            result = prev;
        } while ((prev = prev.getPrev()) != root && prev.getKey() == key);
        return result;
    }

    /**
     * A node that is present at several layers of skip-list.
     */
    private static final class SkipListInternalNode extends SkipListNode {

        private SkipListNode[] nexts;

        private SkipListInternalNode(final long key, final int value) {
            super(key, value);
        }

        @Override
        protected int getLevels() {
            return nexts.length + 1;
        }

        @Override
        protected SkipListNode getNext(int level) {
            return level == 0 ? getNext() : nexts[level - 1];
        }

        @Override
        protected void setNext(final SkipListNode next, int level) {
            if (level == 0) {
                setNext(next);
            } else {
                nexts[level - 1] = next;
            }
        }

        private void setNexts(final SkipListNode[] nexts) {
            this.nexts = nexts;
        }
    }

    /**
     * Bottom node with the only 'next' reference (is present at the bottom layer only).
     */
    private static final class SkipListBottomNode extends SkipListNode {


        private SkipListBottomNode(final long key, final int value) {
            super(key, value);
        }

        @Override
        protected int getLevels() {
            return 1;
        }

        @Override
        protected SkipListNode getNext(int level) {
            checkLevel(level);
            return getNext();
        }

        @Override
        protected void setNext(final SkipListNode next, int level) {
            checkLevel(level);
            setNext(next);
        }

        private static void checkLevel(int level) {
            if (level != 0) {
                throw new IllegalArgumentException("SkipListBottomNode: level can only be equal to 0.");
            }
        }
    }

    private static final class SkipListLevel2Node extends SkipListNode {

        private SkipListNode next2; // level 2 reference

        private SkipListLevel2Node(final long key, final int value) {
            super(key, value);
        }

        @Override
        protected int getLevels() {
            return 2;
        }

        @Override
        protected SkipListNode getNext(int level) {
            if (level == 1) return next2;
            if (level == 0) return getNext();
            throwBadLevel();
            return null;
        }

        @Override
        protected void setNext(final SkipListNode next, int level) {
            if (level == 1) {
                next2 = next;
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

    private static final class SkipListLevel3Node extends SkipListNode {

        private SkipListNode next2; // level 2 reference
        private SkipListNode next3; // level 3 reference

        private SkipListLevel3Node(final long key, final int value) {
            super(key, value);
        }

        @Override
        protected int getLevels() {
            return 3;
        }

        @Override
        protected SkipListNode getNext(int level) {
            switch (level) {
                case 2:
                    return next3;
                case 1:
                    return next2;
                case 0:
                    return getNext();
                default:
                    throwBadLevel();
            }
            return null;
        }

        @Override
        protected void setNext(final SkipListNode next, int level) {
            switch (level) {
                case 2:
                    next3 = next;
                    break;
                case 1:
                    next2 = next;
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
