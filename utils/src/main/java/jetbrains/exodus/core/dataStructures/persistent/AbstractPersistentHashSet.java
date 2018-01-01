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

import jetbrains.exodus.core.dataStructures.Stack;
import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

abstract class AbstractPersistentHashSet<K> implements Iterable<K> {

    static final Object[] EMPTY_TABLE = {};
    @SuppressWarnings("rawtypes")
    static final RootTableNode EMPTY_ROOT = new AbstractPersistentHashSet.RootTableNode(0, EMPTY_TABLE, 0);
    static final int BITS_PER_TABLE = 5;
    static final int BITS_IN_HASH = 32;

    @NotNull
    abstract RootTableNode<K> getRoot();

    public final boolean contains(K key) {
        return getKey(key) != null;
    }

    @Nullable
    public final K getKey(K key) {
        return getRoot().getKey(key, key.hashCode(), 0);
    }

    public final boolean isEmpty() {
        return getRoot().getMask() == 0;
    }

    public final int size() {
        return getRoot().getSize();
    }

    @Override
    public final Iterator<K> iterator() {
        final RootTableNode<K> root = getRoot();
        //noinspection unchecked
        return root.getMask() == 0 ? Collections.EMPTY_LIST.iterator() : new Itr<>(root);
    }

    static class Itr<K> implements Iterator<K> {

        private final Node<K> startingRoot;
        private Stack<TreePos<K>> stack;
        private boolean hasNext;
        private boolean hasNextValid;

        public Itr(Node<K> startingRoot) {
            this.startingRoot = startingRoot;
        }

        @Override
        public boolean hasNext() {
            if (hasNextValid) {
                return hasNext;
            }
            hasNextValid = true;
            if (stack == null) {
                stack = new Stack<>();
                TreePos<K> treePos = new TreePos<>(startingRoot);
                treePos.index = -1;
                stack.push(treePos);
            }
            TreePos<K> treePos = stack.peek();
            treePos.index++;
            while (treePos.node.isOut(treePos.index)) {
                stack.pop();
                if (stack.isEmpty()) {
                    hasNext = false;
                    return hasNext;
                }
                treePos = stack.peek();
                treePos.index++;
            }
            while (true) {
                Object o = treePos.node.get(treePos.index);
                if (!(o instanceof Node)) {
                    hasNext = true;
                    return hasNext;
                }
                //noinspection unchecked
                treePos = new TreePos((Node<K>) o);
                stack.push(treePos);
            }
        }

        @Override
        public K next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            hasNextValid = false;
            TreePos<K> treePos = stack.peek();
            //noinspection unchecked
            return (K) treePos.node.get(treePos.index);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    interface Node<K> {

        @NotNull
        Node<K> insert(@NotNull K key, int hash, int offset, Flag flag);

        @Nullable
        Object remove(@NotNull K key, int hash, int offset);

        @Nullable
        K getKey(@NotNull K key, int hash, int offset);

        void checkNode(int offset);

        RootTableNode<K> asRoot(int size);

        Object get(int index);

        boolean isOut(int index);

        void forEachKey(ObjectProcedure<K> procedure);
    }

    static class TreePos<K> {
        private final Node<K> node;
        private int index;

        TreePos(Node<K> node) {
            this.node = node;
            index = 0;
        }
    }

    static class RootTableNode<K> extends TableNode<K> {

        private final int size;

        RootTableNode(int mask, Object[] table, int size) {
            super(mask, table);
            this.size = size;
        }

        public int getSize() {
            return size;
        }
    }

    @SuppressWarnings("UnnecessaryParentheses")
    static class TableNode<K> implements Node<K> {

        private final int mask;
        private final Object[] table;

        TableNode(int mask, Object[] table) {
            this.mask = mask;
            //noinspection AssignmentToCollectionOrArrayFieldFromParameter
            this.table = table;
        }

        private TableNode(@NotNull K key1, int hash1, @NotNull K key2, int hash2, int offset) {
            int subhash2 = getSubhash(hash2, offset);
            int subhash1 = getSubhash(hash1, offset);
            if (subhash1 == subhash2) {
                mask = 1 << subhash1;
                table = new Object[]{createNode(key1, hash1, key2, hash2, offset + BITS_PER_TABLE)};
            } else {
                mask = (1 << subhash2) + (1 << subhash1);
                if (subhash1 < subhash2) {
                    table = new Object[]{key1, key2};
                } else {
                    table = new Object[]{key2, key1};
                }
            }
        }

        public int getMask() {
            return mask;
        }

        @SuppressWarnings("unchecked")
        @Override
        @NotNull
        public Node<K> insert(@NotNull K key, int hash, int offset, Flag flag) {
            int subhash = getSubhash(hash, offset);
            if ((mask & (1 << subhash)) == 0) {
                flag.flag();
                return cloneAndAdd(key, subhash);
            }
            int index = getPosition(subhash);
            final Object target = table[index];
            final Object result;
            if (target instanceof AbstractPersistentHashSet.Node) {
                //noinspection unchecked
                result = ((Node<K>) target).insert(key, hash, offset + BITS_PER_TABLE, flag);
            } else {
                // target is key
                //noinspection unchecked
                if (target.equals(key)) {
                    result = key;
                } else {
                    flag.flag();
                    result = createNode(((K) target), key, hash, offset + BITS_PER_TABLE);
                }
            }
            //noinspection unchecked
            return (Node<K>) cloneAndReplace(result, index, offset);
        }

        @Override
        @Nullable
        public Object remove(@NotNull K key, int hash, int offset) {
            int subhash = getSubhash(hash, offset);
            if ((mask & (1 << subhash)) == 0) {
                return null;
            }
            int index = getPosition(subhash);
            Object target = table[index];
            if (target instanceof AbstractPersistentHashSet.Node) {
                //noinspection unchecked
                final Object removed = ((Node<K>) target).remove(key, hash, offset + BITS_PER_TABLE);
                if (removed == null) {
                    return null;
                }
                return cloneAndReplace(removed, index, offset);
            } else {
                return target.equals(key) ? cloneAndRemove(subhash, index, offset) : null;
            }
        }

        @Override
        public K getKey(@NotNull K key, int hash, int offset) {
            int subhash = getSubhash(hash, offset);
            if ((mask & (1 << subhash)) == 0) {
                return null;
            }
            int index = getPosition(subhash);
            Object target = table[index];
            if (target instanceof Node) {
                //noinspection unchecked
                return ((Node<K>) target).getKey(key, hash, offset + BITS_PER_TABLE);
            }
            //noinspection unchecked
            return target.equals(key) ? (K) target : null;
        }

        @Override
        public void forEachKey(ObjectProcedure<K> procedure) {
            for (Object target : table) {
                if (target instanceof Node) {
                    //noinspection unchecked
                    ((Node<K>) target).forEachKey(procedure);
                } else {
                    //noinspection unchecked
                    procedure.execute((K) target);
                }
            }
        }

        private TableNode<K> cloneAndAdd(K key, int subhash) {
            int index = getPosition(subhash);
            final int tableLength = table.length;
            Object[] newTable = new Object[tableLength + 1];
            System.arraycopy(table, 0, newTable, 0, index);
            System.arraycopy(table, index, newTable, index + 1, tableLength - index);
            newTable[index] = key;
            return new TableNode<>(mask + (1 << subhash), newTable);
        }

        @NotNull
        private Object cloneAndReplace(@NotNull Object newChild, int index, int offset) {
            final int tableLength = table.length;
            if (offset != 0 && tableLength == 1 && !(newChild instanceof Node)) {
                return newChild;
            }
            Object[] newTable = Arrays.copyOf(table, tableLength);
            newTable[index] = newChild;
            return new TableNode<K>(mask, newTable);
        }

        private Object cloneAndRemove(int subhash, int index, int offset) {
            // mask & (1 << subhash) != 0
            int size = getPosition(1 << BITS_PER_TABLE);
            if (size == 1) {
                return new TableNode<K>(0, EMPTY_TABLE);
            }
            if (size == 2) {
                if (offset == 0 || table[1 - index] instanceof Node) {
                    return new TableNode(mask - (1 << subhash), new Object[]{table[1 - index]});
                } else {
                    return table[1 - index];
                }
            }
            Object[] newTable = new Object[table.length - 1];
            System.arraycopy(table, 0, newTable, 0, index);
            System.arraycopy(table, index + 1, newTable, index, newTable.length - index);
            return new TableNode(mask - (1 << subhash), newTable);
        }

        /**
         * @param subhash amount of lowest bits to consider
         * @return amount of 1s in {@code mask} among {@code subhash} lowest bits
         */
        private int getPosition(int subhash) {
            int m = mask & (subhash == 32 ? -1 : (1 << subhash) - 1);
            m -= (m >>> 1) & 0x55555555;
            m = (m & 0x33333333) + ((m >>> 2) & 0x33333333);
            m = (m + (m >>> 4)) & 0x0F0F0F0F;
            m += m >>> 8;
            return (m + (m >>> 16)) & 0xFF;
        }

        private static <K> Node<K> createNode(K notHashedKey, K hashedKey, int hash, int offset) {
            return createNode(notHashedKey, notHashedKey.hashCode(), hashedKey, hash, offset);
        }

        private static <K> Node<K> createNode(K key1, int hash1, K key2, int hash2, int offset) {
            int subhash1 = getSubhash(hash1, offset);
            int subhash2 = getSubhash(hash2, offset);
            Object[] table;
            if (subhash2 == subhash1) {
                //noinspection unchecked
                table = new Object[]{offset + BITS_PER_TABLE >= BITS_IN_HASH ?
                        new HashCollisionNode<>(key1, key2) :
                        new TableNode<>(key1, hash1, key2, hash2, offset + BITS_PER_TABLE)};
            } else {
                table = subhash2 < subhash1 ? new Object[]{key2, key1} : new Object[]{key1, key2};
            }
            return new TableNode<>((1 << subhash2) | (1 << subhash1), table);
        }

        @SuppressWarnings("UnnecessaryParentheses")
        private static int getSubhash(int hash, int offset) {
            return (hash >>> offset) & ((1 << BITS_PER_TABLE) - 1);
        }

        @Override
        public void checkNode(int offset) {
            if (offset > 0) {
                final int tableLength = table.length;
                if (tableLength == 0 || (tableLength == 1 && !(table[0] instanceof Node))) {
                    throw new RuntimeException("unnecessary use of table node");
                }
            }
            int m = mask;
            for (Object o : table) {
                if (m == 0) {
                    throw new RuntimeException("Inconsistent mask and table");
                }
                m &= m - 1;
                if (o == null) {
                    throw new RuntimeException("Null in table");
                }
                if (o instanceof Node) {
                    //noinspection unchecked
                    ((Node<K>) o).checkNode(offset + BITS_PER_TABLE);
                }
            }
            if (m != 0) {
                throw new RuntimeException("Inconsistent mask and table");
            }
        }

        @Override
        public RootTableNode<K> asRoot(int size) {
            return new RootTableNode<>(mask, table, size);
        }

        @Override
        public Object get(int index) {
            return table[index];
        }

        @Override
        public boolean isOut(int index) {
            return index + 1 > table.length;
        }
    }

    static class HashCollisionNode<K> implements Node<K> {

        private final K[] keys;

        HashCollisionNode(K... keys) {
            //noinspection AssignmentToCollectionOrArrayFieldFromParameter
            this.keys = keys;
        }

        @Override
        @NotNull
        public Node<K> insert(@NotNull K key, int hash, int offset, Flag flag) {
            final int keysLength = keys.length;
            for (int i = 0; i < keysLength; i++) {
                if (keys[i].equals(key)) {
                    K[] newKeys = keys.clone();
                    newKeys[i] = key;
                    //noinspection unchecked
                    return new HashCollisionNode<>(newKeys);
                }
            }
            K[] newKeys = Arrays.copyOf(keys, keysLength + 1);
            newKeys[keysLength] = key;
            flag.flag();
            //noinspection unchecked
            return new HashCollisionNode<>(newKeys);
        }

        @SuppressWarnings("unchecked")
        @Override
        @Nullable
        public Object remove(@NotNull K key, int hash, int offset) {
            final int keysLength = keys.length;
            for (int i = 0; i < keysLength; i++) {
                if (keys[i].equals(key)) {
                    if (keysLength == 2) {
                        return keys[1 - i];
                    }
                    final K[] newKeys = (K[]) new Object[keysLength - 1];
                    for (int j = 0, k = 0; j < keysLength; ++j) {
                        if (j != i) {
                            //noinspection AssignmentToForLoopParameter
                            newKeys[k++] = keys[j];
                        }
                    }
                    return new HashCollisionNode<>(newKeys);
                }
            }
            return null;
        }

        @Override
        public K getKey(@NotNull K key, int hash, int offset) {
            for (K k : keys) {
                if (k.equals(key)) {
                    return k;
                }
            }
            return null;
        }

        @Override
        public void checkNode(int offset) {
            final int keysLength = keys.length;
            if (keysLength < 2) {
                throw new RuntimeException("Unnecessary hash collision node of cardinality " + keysLength);
            }
            for (K key : keys) {
                if (key == null) {
                    throw new RuntimeException("Null in collision list");
                }
            }
        }

        @Override
        public RootTableNode<K> asRoot(int size) {
            throw new UnsupportedOperationException("Unexpected as root!");
        }

        @Override
        public Object get(int index) {
            return keys[index];
        }

        @Override
        public boolean isOut(int index) {
            return index + 1 > keys.length;
        }

        @Override
        public void forEachKey(ObjectProcedure<K> procedure) {
            for (K k : keys) {
                procedure.execute(k);
            }
        }
    }
}
