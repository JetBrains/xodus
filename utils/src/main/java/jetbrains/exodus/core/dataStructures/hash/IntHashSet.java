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
import java.util.Iterator;

public class IntHashSet extends AbstractSet<Integer> {

    private Entry[] table;
    private int capacity;
    private int size;
    private final float loadFactor;
    private int mask;

    public IntHashSet() {
        this(0);
    }

    public IntHashSet(int capacity) {
        this(capacity, HashUtil.DEFAULT_LOAD_FACTOR);
    }

    public IntHashSet(int capacity, float loadFactor) {
        this.loadFactor = loadFactor;
        init(capacity);
    }

    public boolean contains(final int key) {
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
        return contains(((Integer) key).intValue());
    }

    public boolean add(final int key) {
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
        size += 1;

        if (size > capacity) {
            rehash(HashUtil.nextCapacity(capacity));
        }
        return true;
    }

    @Override
    public boolean add(Integer key) {
        return add(key.intValue());
    }

    public boolean remove(int key) {
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
        size -= 1;
        return true;
    }

    @Override
    public boolean remove(Object key) {
        return remove(((Integer) key).intValue());
    }

    @Override
    public Iterator<Integer> iterator() {
        return new HashSetIterator<Integer>() {
            @Override
            public Integer next() {
                return nextEntry().key;
            }
        };
    }

    @Override
    public int size() {
        return size;
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
        this.capacity = capacity;
        size = 0;
    }

    private void rehash(int capacity) {
        final int length = HashUtil.getCeilingPrime((int) (capacity / loadFactor));
        this.capacity = capacity;
        if (length != table.length) {
            final Iterator<Entry> entries = new RehashIterator();
            allocateTable(length);
            final Entry[] table = this.table;
            final int mask = this.mask;
            while (entries.hasNext()) {
                final Entry e = entries.next();
                final int index = HashUtil.indexFor(e.key, length, mask);
                e.hashNext = table[index];
                table[index] = e;
            }
        }
    }

    private final class RehashIterator extends HashSetIterator<Entry> {
        @Override
        public Entry next() {
            return nextEntry();
        }
    }

    private static class Entry {

        private final int key;
        private Entry hashNext;

        private Entry(final int key) {
            this.key = key;
        }
    }

    private abstract class HashSetIterator<T> implements Iterator<T> {

        private final Entry[] table = IntHashSet.this.table;
        private int index = 0;
        private Entry e = null;
        private Entry last;

        HashSetIterator() {
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
            IntHashSet.this.remove(last.key);
            last = null;
        }

        protected Entry nextEntry() {
            final Entry result = last = e;
            initNextEntry();
            return result;
        }

        private void initNextEntry() {
            Entry result = e;
            if (result != null) {
                result = result.hashNext;
            }
            final Entry[] table = this.table;
            while (result == null && index < table.length) {
                result = table[index++];
            }
            e = result;
        }
    }
}
