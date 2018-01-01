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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class PersistentHashMap<K, V> {

    private final PersistentHashSet<Entry<K, V>> set;

    public PersistentHashMap() {
        set = new PersistentHashSet<>();
    }

    private PersistentHashMap(@NotNull final AbstractPersistentHashSet.RootTableNode<Entry<K, V>> root) {
        set = new PersistentHashSet<>(root);
    }

    public ImmutablePersistentHashMap getCurrent() {
        return new ImmutablePersistentHashMap();
    }

    public PersistentHashMap<K, V> getClone() {
        return new PersistentHashMap<>(set.getRoot());
    }

    public MutablePersistentHashMap beginWrite() {
        return new MutablePersistentHashMap();
    }

    public boolean endWrite(@NotNull final MutablePersistentHashMap tree) {
        return set.endWrite(tree);
    }

    public class ImmutablePersistentHashMap extends PersistentHashSet.ImmutablePersistentHashSet<Entry<K, V>> {

        ImmutablePersistentHashMap() {
            super(set.getRoot());
        }

        public V get(@NotNull final K key) {
            final Entry<K, V> entry = getRoot().getKey(new Entry<K, V>(key), key.hashCode(), 0);
            return entry == null ? null : entry.getValue();
        }

        public boolean containsKey(@NotNull final K key) {
            return getRoot().getKey(new Entry<K, V>(key), key.hashCode(), 0) != null;
        }
    }

    public class MutablePersistentHashMap extends PersistentHashSet.MutablePersistentHashSet<Entry<K, V>> {

        MutablePersistentHashMap() {
            super(set);
        }

        public V get(@NotNull final K key) {
            final Entry<K, V> entry = getRoot().getKey(new Entry<K, V>(key), key.hashCode(), 0);
            return entry == null ? null : entry.getValue();
        }

        public boolean containsKey(@NotNull final K key) {
            return getRoot().getKey(new Entry<K, V>(key), key.hashCode(), 0) != null;
        }

        public void put(@NotNull final K key, @NotNull final V value) {
            add(new Entry<>(key, value));
        }

        @SuppressWarnings("MethodOverloadsMethodOfSuperclass")
        public V removeKey(@NotNull final K key) {
            final Entry<K, V> entry = getRoot().getKey(new Entry<K, V>(key), key.hashCode(), 0);
            final V result = entry == null ? null : entry.getValue();
            if (entry != null) {
                remove(entry);
            }
            return result;
        }
    }

    public static class Entry<K, V> {

        @NotNull
        private final K key;
        private final V value;

        private Entry(@NotNull K key) {
            this(key, null);
        }

        private Entry(@NotNull final K key, @Nullable final V value) {
            this.key = key;
            this.value = value;
        }

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override
        public boolean equals(Object obj) {
            //noinspection unchecked
            return key.equals(((Entry<K, V>) obj).key);

        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @NotNull
        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }
}
