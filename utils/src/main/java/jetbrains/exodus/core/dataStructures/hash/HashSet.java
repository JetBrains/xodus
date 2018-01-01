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
import java.util.Collection;
import java.util.Iterator;

public class HashSet<E> extends AbstractSet<E> {

    private Entry<E>[] table;
    private int capacity;
    private int size;
    private final float loadFactor;
    private int mask;
    private boolean holdsNull;

    public HashSet() {
        this(0);
    }

    public HashSet(int capacity) {
        this(capacity, HashUtil.DEFAULT_LOAD_FACTOR);
    }

    public HashSet(int capacity, float loadFactor) {
        this.loadFactor = loadFactor;
        init(capacity);
    }

    public HashSet(final Collection<E> collection) {
        this(collection.size());
        this.addAll(collection);
    }

    @Override
    public boolean contains(Object key) {
        if (key == null) {
            return holdsNull;
        }

        final Entry<E>[] table = this.table;
        final int hash = key.hashCode();
        final int index = HashUtil.indexFor(hash, table.length, mask);

        for (Entry<E> e = table[index]; e != null; e = e.hashNext) {
            final E entryKey;
            if ((entryKey = e.key) == key || entryKey.equals(key)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean add(E key) {
        if (key == null) {
            final boolean wasHoldingNull = holdsNull;
            holdsNull = true;
            if (!wasHoldingNull) {
                size += 1;
            }
            return !wasHoldingNull;
        }

        final Entry<E>[] table = this.table;
        final int hash = key.hashCode();
        final int index = HashUtil.indexFor(hash, table.length, mask);

        for (Entry<E> e = table[index]; e != null; e = e.hashNext) {
            final E entryKey;
            if ((entryKey = e.key) == key || entryKey.equals(key)) {
                return false;
            }
        }

        final Entry<E> e = new Entry<>(key);
        e.hashNext = table[index];
        table[index] = e;
        size += 1;

        if (size > capacity) {
            rehash(HashUtil.nextCapacity(capacity));
        }
        return true;
    }

    @Override
    public boolean remove(Object key) {
        if (key == null) {
            final boolean wasHoldingNull = holdsNull;
            holdsNull = false;
            if (wasHoldingNull) {
                size -= 1;
            }
            return wasHoldingNull;
        }

        final Entry<E>[] table = this.table;
        final int hash = key.hashCode();
        final int index = HashUtil.indexFor(hash, table.length, mask);
        Entry<E> e = table[index];

        if (e == null) return false;

        E entryKey;
        if ((entryKey = e.key) == key || entryKey.equals(key)) {
            table[index] = e.hashNext;
        } else {
            for (; ; ) {
                final Entry<E> last = e;
                e = e.hashNext;
                if (e == null) return false;
                if ((entryKey = e.key) == key || entryKey.equals(key)) {
                    last.hashNext = e.hashNext;
                    break;
                }
            }
        }
        size -= 1;
        return true;
    }

    @Override
    public Iterator<E> iterator() {
        return new HashSetIterator<E>() {
            @Override
            public E next() {
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
        holdsNull = false;
    }

    private void rehash(int capacity) {
        final int length = HashUtil.getCeilingPrime((int) (capacity / loadFactor));
        this.capacity = capacity;
        if (length != table.length) {
            final Iterator<Entry<E>> entries = new RehashIterator();
            allocateTable(length);
            final Entry<E>[] table = this.table;
            final int mask = this.mask;
            while (entries.hasNext()) {
                final Entry<E> e = entries.next();
                final int index = HashUtil.indexFor(e.key.hashCode(), length, mask);
                e.hashNext = table[index];
                table[index] = e;
            }
        }
    }

    private final class RehashIterator extends HashSetIterator<Entry<E>> {
        @Override
        public Entry<E> next() {
            return nextEntry();
        }
    }

    private static class Entry<E> {

        private final E key;
        private Entry<E> hashNext;

        private Entry() {
            key = null;
            hashNext = null;
        }

        private Entry(final E key) {
            this.key = key;
            hashNext = null;
        }
    }

    private abstract class HashSetIterator<T> implements Iterator<T> {

        private final Entry<E>[] table = HashSet.this.table;
        private int index = 0;
        private Entry<E> e = null;
        private Entry<E> last;
        private boolean holdsNull = HashSet.this.holdsNull;

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
            HashSet.this.remove(last.key);
            last = null;
        }

        protected Entry<E> nextEntry() {
            final Entry<E> result = last = e;
            initNextEntry();
            return result;
        }

        private void initNextEntry() {
            Entry<E> result = e;
            if (result != null) {
                result = result.hashNext;
            }
            final Entry<E>[] table = this.table;
            final int length = table.length;
            while (result == null && index < length) {
                result = table[index++];
            }
            if (result == null && holdsNull) {
                holdsNull = false;
                result = new Entry<>();
            }
            e = result;
        }
    }
}
