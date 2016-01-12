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
package jetbrains.exodus.core.dataStructures.persistent;

import jetbrains.exodus.core.dataStructures.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class PersistentLong23TreeMap<V> {

    private final Persistent23Tree<Entry<V>> set;

    public PersistentLong23TreeMap() {
        this(null);
    }

    PersistentLong23TreeMap(@Nullable final AbstractPersistent23Tree.RootNode<Entry<V>> root) {
        set = new Persistent23Tree<>(root);
    }

    public ImmutableMap getCurrent() {
        return new ImmutableMap();
    }

    public PersistentLong23TreeMap<V> getClone() {
        return new PersistentLong23TreeMap<>(set.getRoot());
    }

    public MutableMap beginWrite() {
        return new MutableMap();
    }

    public boolean endWrite(MutableMap tree) {
        return set.endWrite(tree);
    }

    public Entry<V> createEntry(long key) {
        return new Entry<>(key);
    }

    public class ImmutableMap extends Persistent23Tree.ImmutableTree<Entry<V>> {

        ImmutableMap() {
            super(set.getRoot());
        }

        public V get(long key) {
            Node<Entry<V>> root = getRoot();
            if (root == null) {
                return null;
            }
            Entry<V> entry = root.get(new Entry<V>(key));
            return entry == null ? null : entry.getValue();
        }

        public boolean containsKey(long key) {
            final Node<Entry<V>> root = getRoot();
            return root != null && root.get(new Entry<V>(key)) != null;
        }
    }

    public class MutableMap extends Persistent23Tree.MutableTree<Entry<V>> {

        MutableMap() {
            super(set);
        }

        public V get(long key) {
            Node<Entry<V>> root = getRoot();
            if (root == null) {
                return null;
            }
            Entry<V> entry = root.get(new Entry<V>(key));
            return entry == null ? null : entry.getValue();
        }

        public boolean containsKey(long key) {
            return get(key) != null;
        }

        public void put(long key, @NotNull V value) {
            add(new Entry<>(key, value));
        }

        public V remove(long key) {
            RootNode<Entry<V>> root = getRoot();
            if (root == null) {
                return null;
            }
            Pair<Node<Entry<V>>, Entry<V>> removeResult = root.remove(new Entry<V>(key), true);
            if (removeResult == null) {
                return null;
            }
            Node<Entry<V>> res = removeResult.getFirst();
            if (res instanceof RemovedNode) {
                res = res.getFirstChild();
            }
            root = res == null ? null : res.asRoot(root.getSize() - 1);
            setRoot(root);
            return removeResult.getSecond().getValue();
        }
    }

    @SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
    public static class Entry<V> implements Comparable<Entry<V>> {

        private final long key;
        private final V value;

        Entry(long k) {
            this(k, null);
        }

        Entry(long k, @Nullable V v) {
            key = k;
            value = v;
        }

        @Override
        public int compareTo(Entry<V> o) {
            return key > o.key ? 1 : key == o.key ? 0 : -1;
        }

        public V getValue() {
            return value;
        }

        public long getKey() {
            return key;
        }
    }
}