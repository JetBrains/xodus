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
package jetbrains.exodus.core.dataStructures.hash;

import java.util.*;

public abstract class AbstractHashMap<K, V> extends AbstractMap<K, V> {

    protected int size;

    @Override
    public int size() {
        return size;
    }

    @Override
    public void clear() {
        init(0);
    }

    @Override
    public V get(final Object key) {
        Map.Entry<K, V> e = getEntry(key);
        return e == null ? null : e.getValue();
    }

    @Override
    public boolean containsKey(final Object key) {
        return getEntry(key) != null;
    }

    @Override
    public Set<K> keySet() {
        return new KeySet();
    }

    @Override
    public Collection<V> values() {
        return new Values();
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return new EntrySet();
    }

    public boolean forEachKey(final ObjectProcedure<K> procedure) {
        for (final Entry<K, V> entry : entrySet()) {
            if (!procedure.execute(entry.getKey())) return false;
        }
        return true;
    }

    public boolean forEachValue(final ObjectProcedure<V> procedure) {
        for (final Entry<K, V> entry : entrySet()) {
            if (!procedure.execute(entry.getValue())) return false;
        }
        return true;
    }

    public boolean forEachEntry(final ObjectProcedure<Entry<K, V>> procedure) {
        for (final Entry<K, V> entry : entrySet()) {
            if (!procedure.execute(entry)) return false;
        }
        return true;
    }

    public <E extends Throwable> boolean forEachEntry(final ObjectProcedureThrows<Entry<K, V>, E> procedure) throws E {
        for (final Entry<K, V> entry : entrySet()) {
            if (!procedure.execute(entry)) return false;
        }
        return true;
    }

    protected abstract Entry<K, V> getEntry(Object key);

    protected abstract void init(int capacity);

    protected abstract class HashMapIterator {

        protected abstract Entry<K, V> nextEntry();

        protected abstract boolean hasNext();

        protected abstract void remove();

    }

    protected abstract HashMapIterator hashIterator();

    private abstract class HashIteratorDecorator<T> implements Iterator<T> {

        protected final HashMapIterator decorated;

        protected HashIteratorDecorator() {
            decorated = hashIterator();
        }

        @Override
        public boolean hasNext() {
            return decorated.hasNext();
        }

        @Override
        public void remove() {
            decorated.remove();
        }
    }

    private final class EntrySet extends AbstractSet<Entry<K, V>> {

        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return new HashIteratorDecorator<Entry<K, V>>() {
                @Override
                public Entry<K, V> next() {
                    return decorated.nextEntry();
                }
            };
        }

        @Override
        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            final Map.Entry<K, V> rightEntry = (Map.Entry<K, V>) o;
            final Map.Entry<K, V> leftEntry = getEntry(rightEntry.getKey());
            return leftEntry != null && leftEntry.getValue().equals(rightEntry.getValue());
        }

        @Override
        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            final Map.Entry<K, V> e = (Map.Entry<K, V>) o;
            return AbstractHashMap.this.remove(e.getKey()) != null;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public void clear() {
            AbstractHashMap.this.clear();
        }
    }

    private final class KeySet extends AbstractSet<K> {

        @Override
        public Iterator<K> iterator() {
            return new HashIteratorDecorator<K>() {
                @Override
                public K next() {
                    return decorated.nextEntry().getKey();
                }
            };
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean contains(Object o) {
            return containsKey(o);
        }

        @Override
        public boolean remove(Object o) {
            return AbstractHashMap.this.remove(o) != null;
        }

        @Override
        public void clear() {
            AbstractHashMap.this.clear();
        }
    }

    private final class Values extends AbstractCollection<V> {

        @Override
        public Iterator<V> iterator() {
            return new HashIteratorDecorator<V>() {
                @Override
                public V next() {
                    return decorated.nextEntry().getValue();
                }
            };
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean contains(Object o) {
            return containsValue(o);
        }

        @Override
        public void clear() {
            AbstractHashMap.this.clear();
        }
    }

}
