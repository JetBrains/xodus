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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class Persistent23TreeMap<K extends Comparable<K>, V> {

    private final Persistent23Tree<Entry<K, V>> set;

    public Persistent23TreeMap() {
        this(null);
    }

    Persistent23TreeMap(@Nullable final AbstractPersistent23Tree.RootNode<Entry<K, V>> root) {
        set = new Persistent23Tree<>(root);
    }

    public ImmutableMap<K, V> beginRead() {
        return new ImmutableMap<>(set);
    }

    public Persistent23TreeMap<K, V> getClone() {
        return new Persistent23TreeMap<>(set.getRoot());
    }

    public MutableMap<K, V> beginWrite() {
        return new MutableMap<>(set);
    }

    public boolean endWrite(MutableMap<K, V> tree) {
        return set.endWrite(tree);
    }

    public Entry<K, V> createEntry(K key) {
        return new Entry<>(key);
    }

    public static class ImmutableMap<K extends Comparable<K>, V> extends Persistent23Tree.ImmutableTree<Entry<K, V>> {

        ImmutableMap(Persistent23Tree<Entry<K, V>> set) {
            super(set.getRoot());
        }

        public V get(@NotNull K key) {
            Node<Entry<K, V>> root = getRoot();
            if (root == null) {
                return null;
            }
            Entry<K, V> entry = root.get(new Entry<K, V>(key));
            return entry == null ? null : entry.getValue();
        }

        public boolean containsKey(@NotNull K key) {
            final Node<Entry<K, V>> root = getRoot();
            return root != null && root.get(new Entry<K, V>(key)) != null;
        }
    }

    public static class MutableMap<K extends Comparable<K>, V> extends Persistent23Tree.MutableTree<Entry<K, V>> {

        MutableMap(Persistent23Tree<Entry<K, V>> set) {
            super(set);
        }

        public V get(@NotNull K key) {
            Node<Entry<K, V>> root = getRoot();
            if (root == null) {
                return null;
            }
            Entry<K, V> entry = root.get(new Entry<K, V>(key));
            return entry == null ? null : entry.getValue();
        }

        public boolean containsKey(@NotNull K key) {
            return get(key) != null;
        }

        public void put(@NotNull K key, @NotNull V value) {
            add(new Entry<>(key, value));
        }

        public V remove(@NotNull K key) {
            RootNode<Entry<K, V>> root = getRoot();
            if (root == null) {
                return null;
            }
            Pair<Node<Entry<K, V>>, Entry<K, V>> removeResult = root.remove(new Entry<K, V>(key), true);
            if (removeResult == null) {
                return null;
            }
            Node<Entry<K, V>> res = removeResult.getFirst();
            if (res instanceof RemovedNode) {
                res = res.getFirstChild();
            }
            root = res == null ? null : res.asRoot(root.getSize() - 1);
            setRoot(root);
            return removeResult.getSecond().getValue();
        }
    }

    @SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
    public static class Entry<K extends Comparable<K>, V> implements Comparable<Entry<K, V>> {

        private final K key;
        private final V value;

        Entry(K k) {
            this(k, null);
        }

        Entry(K k, @Nullable V v) {
            key = k;
            value = v;
        }

        @Override
        public int compareTo(Entry<K, V> o) {
            return key.compareTo(o.key);
        }

        public V getValue() {
            return value;
        }

        public K getKey() {
            return key;
        }
    }
}