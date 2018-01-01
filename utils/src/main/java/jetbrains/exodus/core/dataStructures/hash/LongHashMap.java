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

import jetbrains.exodus.util.MathUtil;

import java.util.Iterator;
import java.util.Map;

public class LongHashMap<V> extends AbstractHashMap<Long, V> {

    private Entry<V>[] table;
    private int capacity;
    private final float loadFactor;
    private int mask;

    public LongHashMap() {
        this(0);
    }

    public LongHashMap(int capacity) {
        this(capacity, HashUtil.DEFAULT_LOAD_FACTOR);
    }

    public LongHashMap(int capacity, float loadFactor) {
        this.loadFactor = loadFactor;
        init(capacity);
    }

    public V get(final long key) {
        Entry<V> e = getEntry(key);
        return e == null ? null : e.value;
    }

    public V put(final long key, final V value) {
        final Entry<V>[] table = this.table;
        final int index = HashUtil.indexFor(key, table.length, mask);

        for (Entry<V> e = table[index]; e != null; e = e.hashNext) {
            if (e.key == key) {
                return e.setValue(value);
            }
        }

        final Entry<V> e = new Entry<>(key, value);
        e.hashNext = table[index];
        table[index] = e;
        size += 1;

        if (size > capacity) {
            rehash(HashUtil.nextCapacity(capacity));
        }
        return null;
    }

    @Override
    public V put(final Long key, final V value) {
        return put(key.longValue(), value);
    }

    public boolean containsKey(final long key) {
        return get(key) != null;
    }

    public V remove(final long key) {
        final Entry<V>[] table = this.table;
        final int index = HashUtil.indexFor(key, table.length, mask);
        Entry<V> e = table[index];

        if (e == null) return null;

        if (e.key == key) {
            table[index] = e.hashNext;
        } else {
            for (; ; ) {
                final Entry<V> last = e;
                e = e.hashNext;
                if (e == null) return null;
                if (e.key == key) {
                    last.hashNext = e.hashNext;
                    break;
                }
            }
        }
        size -= 1;
        return e.value;
    }

    @Override
    public V remove(Object key) {
        return remove(((Long) key).longValue());
    }

    @Override
    protected Map.Entry<Long, V> getEntry(Object key) {
        return getEntry(((Long) key).longValue());
    }

    @Override
    protected void init(int capacity) {
        if (capacity < HashUtil.MIN_CAPACITY) {
            capacity = HashUtil.MIN_CAPACITY;
        }
        allocateTable(HashUtil.getCeilingPrime((int) (capacity / loadFactor)));
        this.capacity = capacity;
        size = 0;
    }

    @Override
    protected HashMapIterator hashIterator() {
        return new HashIterator();
    }

    private Entry<V> getEntry(final long key) {
        final Entry<V>[] table = this.table;
        final int index = HashUtil.indexFor(key, table.length, mask);

        for (Entry<V> e = table[index]; e != null; e = e.hashNext) {
            if (e.key == key) {
                return e;
            }
        }

        return null;
    }

    private void allocateTable(int length) {
        table = new Entry[length];
        mask = (1 << MathUtil.integerLogarithm(table.length)) - 1;
    }

    private void rehash(int capacity) {
        final int length = HashUtil.getCeilingPrime((int) (capacity / loadFactor));
        this.capacity = capacity;
        if (length != table.length) {
            final Iterator<Map.Entry<Long, V>> entries = entrySet().iterator();
            allocateTable(length);
            final Entry<V>[] table = this.table;
            final int mask = this.mask;
            while (entries.hasNext()) {
                final Entry<V> e = (Entry<V>) entries.next();
                final int index = HashUtil.indexFor(e.key, length, mask);
                e.hashNext = table[index];
                table[index] = e;
            }
        }
    }


    private static class Entry<V> implements Map.Entry<Long, V> {

        private final long key;
        private V value;
        private Entry<V> hashNext;

        private Entry(final long key, final V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public Long getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(final V value) {
            final V result = this.value;
            this.value = value;
            return result;
        }
    }

    private final class HashIterator extends HashMapIterator {

        private final Entry<V>[] table = LongHashMap.this.table;
        private int index = 0;
        private Entry<V> e = null;
        private Entry<V> last;

        HashIterator() {
            initNextEntry();
        }

        @Override
        public boolean hasNext() {
            return e != null;
        }

        @Override
        public void remove() {
            if (last == null) {
                throw new IllegalStateException();
            }
            LongHashMap.this.remove(last.key);
            last = null;
        }

        @Override
        protected Entry<V> nextEntry() {
            final Entry<V> result = last = e;
            initNextEntry();
            return result;
        }

        private void initNextEntry() {
            Entry<V> result = e;
            if (result != null) {
                result = result.hashNext;
            }
            final Entry<V>[] table = this.table;
            while (result == null && index < table.length) {
                result = table[index++];
            }
            e = result;
        }
    }
}
