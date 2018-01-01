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
import org.jetbrains.annotations.Nullable;

import java.util.AbstractSet;
import java.util.Iterator;

public class LinkedHashSet<E> extends AbstractSet<E> {

    private Entry<E>[] table;
    private Entry<E> top;
    private Entry<E> back;
    private int capacity;
    private int size;
    private final float loadFactor;
    private int mask;
    private boolean holdsNull;

    public LinkedHashSet() {
        this(0);
    }

    public LinkedHashSet(int capacity) {
        this(capacity, HashUtil.DEFAULT_LOAD_FACTOR);
    }

    public LinkedHashSet(int capacity, float loadFactor) {
        this.loadFactor = loadFactor;
        init(capacity);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean contains(final Object key) {
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
        final Entry<E> top = this.top;
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
        unlink(e);
        size -= 1;
        return true;
    }

    @Override
    public Iterator<E> iterator() {
        return new LinkedHashIterator();
    }

    @Nullable
    public E getTop() {
        return top == null ? null : top.key;
    }

    @Nullable
    public E getBack() {
        return back == null ? null : back.key;
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
        holdsNull = false;
    }

    private void unlink(final Entry<E> e) {
        final Entry<E> prev = e.previous;
        final Entry<E> next = e.next;
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
            final Entry<E>[] table = this.table;
            final int mask = this.mask;
            for (Entry<E> e = back; e != null; e = e.previous) {
                final int index = HashUtil.indexFor(e.key.hashCode(), length, mask);
                e.hashNext = table[index];
                table[index] = e;
            }
        }
    }


    private static class Entry<E> {

        private final E key;
        private Entry<E> next;
        private Entry<E> previous;
        private Entry<E> hashNext;

        private Entry() {
            key = null;
        }

        private Entry(final E key) {
            this.key = key;
        }
    }

    private class LinkedHashIterator implements Iterator<E> {

        private Entry<E> e;
        private Entry<E> last;

        private LinkedHashIterator() {
            final Entry<E> back = LinkedHashSet.this.back;
            if (holdsNull) {
                e = new Entry<>();
                e.previous = back;
            } else {
                e = back;
            }
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
            LinkedHashSet.this.remove(last.key);
            last = null;
        }

        @Override
        public E next() {
            final Entry<E> result = last = e;
            e = result.previous;
            return result.key;
        }
    }
}
