/*
 * Copyright 2010 - 2024 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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

public class IntLinkedHashMap<V> extends AbstractHashMap<Integer, V> {

    private Entry<V>[] table;
    private Entry<V> top;
    private Entry<V> back;
    private int capacity;
    private final float loadFactor;
    private int mask;

    public IntLinkedHashMap() {
        this(0);
    }

    public IntLinkedHashMap(int capacity) {
        this(capacity, HashUtil.DEFAULT_LOAD_FACTOR);
    }

    public IntLinkedHashMap(int capacity, float loadFactor) {
        this.loadFactor = loadFactor;
        init(capacity);
    }

    public V get(final int key) {
        Entry<V> e = getEntry(key);
        return e == null ? null : e.value;
    }

    public V put(final int key, final V value) {
        final Entry<V>[] table = this.table;
        final int index = HashUtil.indexFor(key, table.length, mask);

        for (Entry<V> e = table[index]; e != null; e = e.hashNext) {
            if (e.key == key) {
                moveToTop(e);
                return e.setValue(value);
            }
        }

        final Entry<V> e = new Entry<>(key, value);
        e.hashNext = table[index];
        table[index] = e;
        final Entry<V> top = this.top;
        e.next = top;
        if (top != null) {
            top.previous = e;
        } else {
            back = e;
        }
        this.top = e;
        _size += 1;

        if (removeEldestEntry(back)) {
            remove(back.key);
        } else if (_size > capacity) {
            rehash(HashUtil.nextCapacity(capacity));
        }
        return null;
    }

    @Override
    public V put(final Integer key, final V value) {
        return put(key.intValue(), value);
    }

    public boolean containsKey(final int key) {
        return getEntry(key) != null;
    }

    public V remove(final int key) {
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
        unlink(e);
        _size -= 1;
        return e.value;
    }

    @Override
    public V remove(Object key) {
        return remove(((Integer) key).intValue());
    }


    protected boolean removeEldestEntry(Map.Entry<Integer, V> eldest) {
        return false;
    }

    @Override
    protected Map.Entry<Integer, V> getEntry(Object key) {
        return getEntry(((Integer) key).intValue());
    }

    @Override
    protected void init(int capacity) {
        if (capacity < HashUtil.MIN_CAPACITY) {
            capacity = HashUtil.MIN_CAPACITY;
        }
        allocateTable(HashUtil.getCeilingPrime((int) (capacity / loadFactor)));
        top = back = null;
        this.capacity = capacity;
        _size = 0;
    }

    @Override
    protected HashMapIterator hashIterator() {
        return new HashIterator();
    }

    private Entry<V> getEntry(final int key) {
        final Entry<V>[] table = this.table;
        final int index = HashUtil.indexFor(key, table.length, mask);

        for (Entry<V> e = table[index]; e != null; e = e.hashNext) {
            if (e.key == key) {
                moveToTop(e);
                return e;
            }
        }

        return null;
    }

    private void allocateTable(int length) {
        table = new Entry[length];
        mask = (1 << MathUtil.integerLogarithm(table.length)) - 1;
    }

    private void moveToTop(final Entry<V> e) {
        final Entry<V> top = this.top;
        if (top != e) {
            final Entry<V> prev = e.previous;
            final Entry<V> next = e.next;
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

    private void unlink(final Entry<V> e) {
        final Entry<V> prev = e.previous;
        final Entry<V> next = e.next;
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
            final Entry<V>[] table = this.table;
            final int mask = this.mask;
            for (Entry<V> e = back; e != null; e = e.previous) {
                final int index = HashUtil.indexFor(e.key, length, mask);
                e.hashNext = table[index];
                table[index] = e;
            }
        }
    }


    private static class Entry<V> implements Map.Entry<Integer, V> {

        private final int key;
        private V value;
        private Entry<V> next;
        private Entry<V> previous;
        private Entry<V> hashNext;

        private Entry(final int key, final V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public Integer getKey() {
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

        private Entry<V> e = top;
        private Entry<V> last;

        @Override
        public boolean hasNext() {
            return e != null;
        }

        @Override
        public void remove() {
            if (last == null) {
                throw new IllegalStateException();
            }
            IntLinkedHashMap.this.remove(last.key);
            last = null;
        }

        @Override
        protected Entry<V> nextEntry() {
            final Entry<V> result = last = e;
            e = result.next;
            return result;
        }
    }
}