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

import java.util.Map;

public class LinkedHashMap<K, V> extends AbstractHashMap<K, V> {

    private Entry<K, V>[] table;
    private Entry<K, V> top;
    private Entry<K, V> back;
    private int capacity;
    private final float loadFactor;
    private int mask;

    public LinkedHashMap() {
        this(0);
    }

    public LinkedHashMap(int capacity) {
        this(capacity, HashUtil.DEFAULT_LOAD_FACTOR);
    }

    public LinkedHashMap(int capacity, float loadFactor) {
        this.loadFactor = loadFactor;
        init(capacity);
    }

    @Override
    public V put(final K key, final V value) {
        final Entry<K, V>[] table = this.table;
        final int hash = key.hashCode();
        final int index = HashUtil.indexFor(hash, table.length, mask);

        for (Entry<K, V> e = table[index]; e != null; e = e.hashNext) {
            final K entryKey;
            if ((entryKey = e.key) == key || entryKey.equals(key)) {
                moveToTop(e);
                return e.setValue(value);
            }
        }

        final Entry<K, V> e = new Entry<>(key, value);
        e.hashNext = table[index];
        table[index] = e;
        final Entry<K, V> top = this.top;
        e.next = top;
        if (top != null) {
            top.previous = e;
        } else {
            back = e;
        }
        this.top = e;
        size += 1;

        if (removeEldestEntry(back)) {
            remove(eldestKey());
        } else if (size > capacity) {
            rehash(HashUtil.nextCapacity(capacity));
        }
        return null;
    }

    @Override
    public V remove(final Object key) {
        final Entry<K, V>[] table = this.table;
        final int hash = key.hashCode();
        final int index = HashUtil.indexFor(hash, table.length, mask);
        Entry<K, V> e = table[index];

        if (e == null) return null;

        K entryKey;
        if ((entryKey = e.key) == key || entryKey.equals(key)) {
            table[index] = e.hashNext;
        } else {
            for (; ; ) {
                final Entry<K, V> last = e;
                e = e.hashNext;
                if (e == null) return null;
                if ((entryKey = e.key) == key || entryKey.equals(key)) {
                    last.hashNext = e.hashNext;
                    break;
                }
            }
        }
        unlink(e);
        size -= 1;
        return e.value;
    }

    public V removeEldest() {
        return remove(eldestKey());
    }

    private K eldestKey() {
        return back.key;
    }

    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return false;
    }

    @Override
    protected Map.Entry<K, V> getEntry(Object key) {
        final Entry<K, V>[] table = this.table;
        final int hash = key.hashCode();
        final int index = HashUtil.indexFor(hash, table.length, mask);

        for (Entry<K, V> e = table[index]; e != null; e = e.hashNext) {
            final K entryKey;
            if ((entryKey = e.key) == key || entryKey.equals(key)) {
                moveToTop(e);
                return e;
            }
        }

        return null;
    }

    @Override
    protected void init(int capacity) {
        if (capacity < HashUtil.MIN_CAPACITY) {
            capacity = HashUtil.MIN_CAPACITY;
        }
        allocateTable(HashUtil.getCeilingPrime((int) (capacity / loadFactor)));
        top = back = null;
        this.capacity = capacity;
        size = 0;
    }

    @Override
    protected HashMapIterator hashIterator() {
        return new HashIterator();
    }

    private void allocateTable(int length) {
        table = new Entry[length];
        mask = (1 << MathUtil.integerLogarithm(table.length)) - 1;
    }

    private void moveToTop(final Entry<K, V> e) {
        final Entry<K, V> top = this.top;
        if (top != e) {
            final Entry<K, V> prev = e.previous;
            final Entry<K, V> next = e.next;
            prev.next = next;
            if (next != null) {
                next.previous = prev;
            } else {
                back = prev;
            }
            top.previous = e;
            e.next = top;
            e.previous = null;
            this.top = e;
        }
    }

    private void unlink(final Entry<K, V> e) {
        final Entry<K, V> prev = e.previous;
        final Entry<K, V> next = e.next;
        if (prev != null) {
            prev.next = next;
        } else {
            top = next;
        }
        if (next != null) {
            next.previous = prev;
        } else {
            back = prev;
        }
    }

    private void rehash(int capacity) {
        final int length = HashUtil.getCeilingPrime((int) (capacity / loadFactor));
        this.capacity = capacity;
        if (length != table.length) {
            allocateTable(length);
            final Entry<K, V>[] table = this.table;
            final int mask = this.mask;
            for (Entry<K, V> e = back; e != null; e = e.previous) {
                final int index = HashUtil.indexFor(e.key.hashCode(), length, mask);
                e.hashNext = table[index];
                table[index] = e;
            }
        }
    }


    private static class Entry<K, V> implements Map.Entry<K, V> {

        private final K key;
        private V value;
        private Entry<K, V> next;
        private Entry<K, V> previous;
        private Entry<K, V> hashNext;

        private Entry(final K key, final V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
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

        private Entry<K, V> e = top;
        private Entry<K, V> last;

        @Override
        public boolean hasNext() {
            return e != null;
        }

        @Override
        public void remove() {
            if (last == null) {
                throw new IllegalStateException();
            }
            LinkedHashMap.this.remove(last.key);
            last = null;
        }

        @Override
        protected Entry<K, V> nextEntry() {
            final Entry<K, V> result = last = e;
            e = result.next;
            return result;
        }
    }
}