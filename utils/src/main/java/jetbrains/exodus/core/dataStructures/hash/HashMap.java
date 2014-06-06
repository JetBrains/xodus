/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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

import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

public class HashMap<K, V> extends AbstractHashMap<K, V> implements Serializable {

    private Entry<K, V>[] table;
    private int capacity;
    private final float loadFactor;
    private int shift;
    private int mask;
    private Entry<K, V> nullEntry;

    public HashMap() {
        this(0);
    }

    public HashMap(int capacity) {
        this(capacity, HashUtil.DEFAULT_LOAD_FACTOR);
    }

    public HashMap(int capacity, float loadFactor) {
        this.loadFactor = loadFactor;
        init(capacity);
    }

    @Override
    public V put(@Nullable final K key, final V value) {
        if (key == null) {
            if (nullEntry == null) {
                size += 1;
                nullEntry = new Entry<K, V>(null, value);
                return null;
            }
            return nullEntry.setValue(value);
        }
        final Entry<K, V>[] table = this.table;
        final int hash = key.hashCode();
        final int index = HashUtil.indexFor(hash, table.length, shift, mask);

        for (Entry<K, V> e = table[index]; e != null; e = e.hashNext) {
            final K entryKey;
            if (e.keyHash == hash && ((entryKey = e.key) == key || entryKey.equals(key))) {
                return e.setValue(value);
            }
        }

        final Entry<K, V> e = new Entry<K, V>(key, value);
        e.hashNext = table[index];
        table[index] = e;
        size += 1;

        if (size > capacity) {
            rehash(HashUtil.nextCapacity(capacity));
        }
        return null;
    }

    @Override
    public V remove(final Object key) {
        if (key == null) {
            if (nullEntry != null) {
                size -= 1;
                V hadNullValue = nullEntry.value;
                nullEntry = null;
                return hadNullValue;
            }
            return null;
        }
        final Entry<K, V>[] table = this.table;
        final int hash = key.hashCode();
        final int index = HashUtil.indexFor(hash, table.length, shift, mask);
        Entry<K, V> e = table[index];

        if (e == null) return null;

        K entryKey;
        if (e.keyHash == hash && ((entryKey = e.key) == key || entryKey.equals(key))) {
            table[index] = e.hashNext;
        } else {
            for (; ; ) {
                final Entry<K, V> last = e;
                e = e.hashNext;
                if (e == null) return null;
                if (e.keyHash == hash && ((entryKey = e.key) == key || entryKey.equals(key))) {
                    last.hashNext = e.hashNext;
                    break;
                }
            }
        }
        size -= 1;
        return e.value;
    }

    @Override
    protected Map.Entry<K, V> getEntry(final Object key) {
        if (key == null) {
            return nullEntry;
        }
        final Entry<K, V>[] table = this.table;
        final int hash = key.hashCode();
        final int index = HashUtil.indexFor(hash, table.length, shift, mask);

        for (Entry<K, V> e = table[index]; e != null; e = e.hashNext) {
            final K entryKey;
            if (e.keyHash == hash && ((entryKey = e.key) == key || entryKey.equals(key))) {
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
        this.capacity = capacity;
        size = 0;
    }

    @Override
    protected HashMapIterator hashIterator() {
        return new HashIterator();
    }

    private void allocateTable(int length) {
        table = new Entry[length];
        shift = HashUtil.shift(table.length);
        mask = (1 << shift) - 1;
    }

    private void rehash(int capacity) {
        final int length = HashUtil.getCeilingPrime((int) (capacity / loadFactor));
        this.capacity = capacity;
        if (length != table.length) {
            final Iterator<Map.Entry<K, V>> entries = entrySet().iterator();
            allocateTable(length);
            final Entry<K, V>[] table = this.table;
            final int shift = this.shift;
            final int mask = this.mask;
            while (entries.hasNext()) {
                final Entry<K, V> e = (Entry<K, V>) entries.next();
                if (e.key != null) {
                    final int index = HashUtil.indexFor(e.keyHash, length, shift, mask);
                    e.hashNext = table[index];
                    table[index] = e;
                }
            }
        }
    }


    private static class Entry<K, V> implements Map.Entry<K, V>, Serializable {

        private final K key;
        private final int keyHash;
        private V value;
        private Entry<K, V> hashNext;

        private Entry(final K key, final V value) {
            this.key = key;
            keyHash = key == null ? 0 : key.hashCode();
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

        private final Entry<K, V>[] table = HashMap.this.table;
        private int index = -1;
        private Entry<K, V> e = null;
        private Entry<K, V> last;

        HashIterator() {
            initNextEntry();
        }

        @Override
        protected boolean hasNext() {
            return e != null;
        }

        @Override
        protected void remove() {
            if (last == null) {
                throw new IllegalStateException();
            }
            HashMap.this.remove(last.key);
            last = null;
        }

        @Override
        protected Entry<K, V> nextEntry() {
            final Entry<K, V> result = last = e;
            initNextEntry();
            return result;
        }

        private void initNextEntry() {
            Entry<K, V> result = e;
            if (result != null) {
                result = result.hashNext;
            }
            if (index < 0) {
                result = nullEntry;
                index = 0;
            }
            final Entry<K, V>[] table = this.table;
            while (result == null && index < table.length) {
                result = table[index++];
            }
            e = result;
        }
    }
}
