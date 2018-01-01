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

import java.util.AbstractSet;

public class LongLinkedHashSet extends AbstractSet<Long> implements LongSet {

    private Entry[] table;
    private Entry top;
    private Entry back;
    private int capacity;
    private int size;
    private final float loadFactor;
    private int mask;

    public LongLinkedHashSet() {
        this(0);
    }

    public LongLinkedHashSet(int capacity) {
        this(capacity, HashUtil.DEFAULT_LOAD_FACTOR);
    }

    public LongLinkedHashSet(int capacity, float loadFactor) {
        this.loadFactor = loadFactor;
        init(capacity);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public long[] toLongArray() {
        if (size == 0) return EMPTY_ARRAY;
        final long[] result = new long[size];
        int i = 0;
        final LongIterator itr = iterator();
        while (itr.hasNext()) {
            result[i++] = itr.nextLong();
        }
        return result;
    }

    @Override
    public boolean contains(final long key) {
        final Entry[] table = this.table;
        final int index = HashUtil.indexFor(key, table.length, mask);

        for (Entry e = table[index]; e != null; e = e.hashNext) {
            if (e.key == key) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean contains(final Object key) {
        return contains(((Long) key).longValue());
    }

    @Override
    public boolean add(final long key) {
        final Entry[] table = this.table;
        final int index = HashUtil.indexFor(key, table.length, mask);

        for (Entry e = table[index]; e != null; e = e.hashNext) {
            if (e.key == key) {
                return false;
            }
        }

        final Entry e = new Entry(key);
        e.hashNext = table[index];
        table[index] = e;
        final Entry top = this.top;
        e.next = top;
        if (top != null) {
            top.previous = e;
        } else {
            back = e;
        }
        this.top = e;
        size += 1;

        if (size > capacity) {
            rehash(HashUtil.nextCapacity(capacity));
        }
        return true;
    }

    @Override
    public boolean add(Long key) {
        return add(key.longValue());
    }

    @Override
    public boolean remove(final long key) {
        final Entry[] table = this.table;
        final int index = HashUtil.indexFor(key, table.length, mask);
        Entry e = table[index];

        if (e == null) return false;

        if (e.key == key) {
            table[index] = e.hashNext;
        } else {
            for (; ; ) {
                final Entry last = e;
                e = e.hashNext;
                if (e == null) return false;
                if (e.key == key) {
                    last.hashNext = e.hashNext;
                    break;
                }
            }
        }
        unlink(e);
        size -= 1;
        return true;
    }

    @Override
    public boolean remove(Object key) {
        return remove(((Long) key).longValue());
    }

    @Override
    public LongIterator iterator() {
        return new LinkedHashIterator();
    }

    private void allocateTable(int length) {
        table = new Entry[length];
        mask = (1 << MathUtil.integerLogarithm(table.length)) - 1;
    }

    private void init(int capacity) {
        if (capacity < HashUtil.MIN_CAPACITY) {
            capacity = HashUtil.MIN_CAPACITY;
        }
        allocateTable(HashUtil.getCeilingPrime((int) (capacity / loadFactor)));
        top = back = null;
        this.capacity = capacity;
        size = 0;
    }

    private void unlink(final Entry e) {
        final Entry prev = e.previous;
        final Entry next = e.next;
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
            final Entry[] table = this.table;
            final int mask = this.mask;
            for (Entry e = back; e != null; e = e.previous) {
                final int index = HashUtil.indexFor(e.key, length, mask);
                e.hashNext = table[index];
                table[index] = e;
            }
        }
    }


    private static class Entry {

        private final long key;
        private Entry next;
        private Entry previous;
        private Entry hashNext;

        private Entry(final long key) {
            this.key = key;
        }
    }

    private class LinkedHashIterator implements LongIterator {

        private Entry e;
        private Entry last;

        private LinkedHashIterator() {
            e = back;
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
            LongLinkedHashSet.this.remove(last.key);
            last = null;
        }

        @Override
        public Long next() {
            return nextLong();
        }

        @Override
        public long nextLong() {
            final Entry result = last = e;
            e = result.previous;
            return result.key;
        }
    }
}
