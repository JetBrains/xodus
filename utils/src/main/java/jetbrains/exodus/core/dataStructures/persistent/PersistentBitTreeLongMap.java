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

import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class PersistentBitTreeLongMap<V> implements PersistentLongMap<V> {

    private static final int BITS_PER_ENTRY = 10;
    private static final int ELEMENTS_PER_ENTRY = 1 << BITS_PER_ENTRY;
    private static final int MASK = ELEMENTS_PER_ENTRY - 1;
    private static final BitSet FAKE_BITS = new BitSet();
    private static final Object[] FAKE_DATA = new Object[0];

    private final Root<V> root;

    public PersistentBitTreeLongMap() {
        this.root = new Root<>(new Persistent23Tree<Entry>(), 0);
    }

    private PersistentBitTreeLongMap(Root<V> root) {
        this.root = root;
    }

    @Override
    public PersistentLongMap.ImmutableMap<V> beginRead() {
        return new ImmutableMap<>(root.map.beginRead(), root.size);
    }

    @Override
    public PersistentLongMap<V> getClone() {
        return new PersistentBitTreeLongMap<>(root.getClone());
    }

    @Override
    public PersistentLongMap.MutableMap<V> beginWrite() {
        return new MutableMap<>(root.map.beginWrite(), root.size, this);
    }

    private static long getEntryIndex(long value) {
        return value >> BITS_PER_ENTRY;
    }

    @NotNull
    private static PersistentBitTreeLongMap.Entry makeIndexEntry(long index) {
        return new Entry(index, null);
    }

    protected static class ImmutableMap<V> implements PersistentLongMap.ImmutableMap<V> {
        @NotNull
        protected final AbstractPersistent23Tree<Entry> map;
        protected final int size;

        ImmutableMap(@NotNull AbstractPersistent23Tree<Entry> map, int size) {
            this.map = map;
            this.size = size;
        }

        @Nullable
        private Entry getEntryByIndex(long index) {
            final AbstractPersistent23Tree.RootNode<Entry> root = map.getRoot();
            if (root == null) {
                return null;
            }
            return root.getByWeight(index);
        }

        @SuppressWarnings("unchecked")
        @Override
        public V get(long key) {
            final Entry entry = getEntryByIndex(getEntryIndex(key));
            if (entry == null) {
                return null;
            }
            return (V) entry.data[(int) (key & MASK)];
        }

        @Override
        public boolean containsKey(long key) {
            final Entry entry = getEntryByIndex(getEntryIndex(key));
            return entry != null && entry.data[(int) (key & MASK)] != null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public PersistentLongMap.Entry<V> getMinimum() {
            final Entry entry = map.getMinimum();
            if (entry == null) {
                return null;
            }
            int index = entry.bits.nextSetBit(0);
            if (index == -1) {
                throw new IllegalStateException("unexpected empty entry");
            }
            return new LongMapEntry<>(index + (entry.index << BITS_PER_ENTRY), (V) entry.data[index]);
        }

        @NotNull
        @Override
        public Iterator<PersistentLongMap.Entry<V>> iterator() {
            return new ItemIterator<>(map);
        }

        @Override
        public Iterator<PersistentLongMap.Entry<V>> reverseIterator() {
            return new ReverseItemIterator<>(map);
        }

        @Override
        public Iterator<PersistentLongMap.Entry<V>> tailEntryIterator(long startingKey) {
            return new ItemTailIterator<>(map, startingKey);
        }

        @Override
        public Iterator<PersistentLongMap.Entry<V>> tailReverseEntryIterator(long startingKey) {
            return new ReverseItemTailIterator<>(map, startingKey);
        }

        @Override
        public boolean isEmpty() {
            return size == 0;
        }

        @Override
        public int size() {
            return size;
        }
    }

    protected static class MutableMap<V> implements PersistentLongMap.MutableMap<V>, RootHolder {

        @NotNull
        private Persistent23Tree.MutableTree<Entry> mutableMap;
        private int size;
        private final PersistentBitTreeLongMap baseMap;

        MutableMap(@NotNull Persistent23Tree.MutableTree<Entry> mutableMap, int size, PersistentBitTreeLongMap baseMap) {
            this.mutableMap = mutableMap;
            this.size = size;
            this.baseMap = baseMap;
        }

        @Override
        public AbstractPersistent23Tree.RootNode<Entry> getRoot() {
            return mutableMap.getRoot();
        }

        @Nullable
        private Entry getEntryByIndex(long index) {
            final AbstractPersistent23Tree.RootNode<Entry> root = getRoot();
            if (root == null) {
                return null;
            }
            return root.getByWeight(index);
        }

        @SuppressWarnings("unchecked")
        @Override
        public V get(long key) {
            final Entry entry = getEntryByIndex(getEntryIndex(key));
            if (entry == null) {
                return null;
            }
            return (V) entry.data[(int) (key & MASK)];
        }

        @Override
        public boolean containsKey(long key) {
            final Entry entry = getEntryByIndex(getEntryIndex(key));
            return entry != null && entry.data[(int) (key & MASK)] != null;
        }

        @Override
        public boolean isEmpty() {
            return size == 0;
        }

        @Override
        public int size() {
            return size;
        }

        @SuppressWarnings("unchecked")
        @Override
        public PersistentLongMap.Entry<V> getMinimum() {
            final Entry entry = mutableMap.getMinimum();
            if (entry == null) {
                return null;
            }
            int index = entry.bits.nextSetBit(0);
            if (index == -1) {
                throw new IllegalStateException("unexpected empty entry");
            }
            return new LongMapEntry<>(index + (entry.index << BITS_PER_ENTRY), (V) entry.data[index]);
        }

        @NotNull
        @Override
        public Iterator<PersistentLongMap.Entry<V>> iterator() {
            return new ItemIterator<>(mutableMap);
        }

        @Override
        public Iterator<PersistentLongMap.Entry<V>> reverseIterator() {
            return new ReverseItemIterator<>(mutableMap);
        }

        @Override
        public Iterator<PersistentLongMap.Entry<V>> tailEntryIterator(long startingKey) {
            return new ItemTailIterator<>(mutableMap, startingKey);
        }

        @Override
        public Iterator<PersistentLongMap.Entry<V>> tailReverseEntryIterator(long startingKey) {
            return new ReverseItemTailIterator<>(mutableMap, startingKey);
        }

        @Override
        public void put(long key, @NotNull V value) {
            final long index = getEntryIndex(key);
            Entry entry = getEntryByIndex(index);
            int bitIndex = (int) (key & MASK);
            if (entry == null) {
                entry = new Entry(index);
                mutableMap.add(entry);
                size++;
            } else {
                final Entry copy = new Entry(index, entry);
                mutableMap.add(copy);
                entry = copy;
                if (!entry.bits.get(bitIndex)) {
                    size++;
                }
            }
            entry.bits.set(bitIndex);
            entry.data[bitIndex] = value;
        }

        @SuppressWarnings("unchecked")
        @Override
        public V remove(long key) {
            final long index = getEntryIndex(key);
            Entry entry = getEntryByIndex(index);
            if (entry == null) {
                return null;
            }
            int bitIndex = (int) (key & MASK);
            if (entry.bits.get(bitIndex)) {
                size--;
            } else {
                return null;
            }
            final Object result = entry.data[bitIndex];
            final Entry copy = new Entry(index, entry);
            copy.bits.clear(bitIndex);
            if (copy.bits.isEmpty()) {
                mutableMap.exclude(entry);
            } else {
                copy.data[bitIndex] = null;
                mutableMap.add(copy);
            }
            return (V) result;
        }

        @Override
        public boolean endWrite() {
            if (!mutableMap.endWrite()) {
                return false;
            }
            // TODO: consistent size update
            baseMap.root.size = size;
            return true;
        }

        @Override
        public void testConsistency() {
            mutableMap.testConsistency();
        }
    }

    protected static class Entry implements LongComparable<Entry> {
        private final long index;
        @NotNull
        private final BitSet bits;
        private final Object[] data;

        public Entry(long min) {
            this.index = min;
            this.data = new Object[ELEMENTS_PER_ENTRY];
            this.bits = new BitSet(ELEMENTS_PER_ENTRY);
        }

        public Entry(long min, Entry other) {
            this.index = min;
            if (other != null) {
                this.bits = new BitSet(ELEMENTS_PER_ENTRY);
                this.bits.or(other.bits);
                this.data = new Object[ELEMENTS_PER_ENTRY];
                System.arraycopy(other.data, 0, this.data, 0, ELEMENTS_PER_ENTRY);
            } else {
                this.bits = FAKE_BITS;
                this.data = FAKE_DATA;
            }
        }

        @Override
        public long getWeight() {
            return index;
        }

        @Override
        public int compareTo(@NotNull Entry o) {
            final long otherMin = o.index;
            return index > otherMin ? 1 : index == otherMin ? 0 : -1;
        }
    }

    protected static class Root<V> {

        @NotNull
        private final Persistent23Tree<Entry> map;
        private int size;

        Root(@NotNull Persistent23Tree<Entry> map, int size) {
            this.map = map;
            this.size = size;
        }

        public Root<V> getClone() {
            return new Root<>(map.getClone(), size);
        }
    }

    private final static class ItemIterator<V> implements Iterator<PersistentLongMap.Entry<V>> {
        @NotNull
        private final Iterator<Entry> iterator;
        private Entry currentEntry = null;
        private long currentEntryBase = 0;
        private int next = -1;

        ItemIterator(AbstractPersistent23Tree<Entry> tree) {
            iterator = tree.iterator();
        }

        @SuppressWarnings("unchecked")
        @Override
        public PersistentLongMap.Entry<V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final int index = this.next;
            final long key = index + currentEntryBase;
            final Object result = currentEntry.data[index];
            this.next = currentEntry.bits.nextSetBit(index + 1);
            return new LongMapEntry<>(key, (V) result);
        }

        @Override
        public boolean hasNext() {
            return next != -1 || fetchEntry();
        }

        private boolean fetchEntry() {
            while (iterator.hasNext()) {
                final Entry entry = iterator.next();
                final int nextIndex = entry.bits.nextSetBit(0);
                if (nextIndex != -1) {
                    currentEntry = entry;
                    currentEntryBase = entry.index << BITS_PER_ENTRY;
                    next = nextIndex;
                    return true;
                }
            }
            return false;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private final static class ItemTailIterator<V> implements Iterator<PersistentLongMap.Entry<V>> {
        @NotNull
        private final Iterator<Entry> iterator;
        private final long startingEntryIndex;
        private int startingIndex;
        private Entry currentEntry = null;
        private long currentEntryBase = 0;
        private int next = -1;

        ItemTailIterator(AbstractPersistent23Tree<Entry> tree, long startingKey) {
            this.startingEntryIndex = getEntryIndex(startingKey);
            this.iterator = tree.tailIterator(makeIndexEntry(startingEntryIndex));
            this.startingIndex = (int) (startingKey & MASK);
        }

        @SuppressWarnings("unchecked")
        @Override
        public PersistentLongMap.Entry<V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final int index = this.next;
            final long key = index + currentEntryBase;
            final Object result = currentEntry.data[index];
            this.next = currentEntry.bits.nextSetBit(index + 1);
            return new LongMapEntry<>(key, (V) result);
        }

        @Override
        public boolean hasNext() {
            return next != -1 || fetchEntry();
        }

        private boolean fetchEntry() {
            while (iterator.hasNext()) {
                final Entry entry = iterator.next();
                int fromIndex = startingIndex;
                if (fromIndex != 0) {
                    if (startingEntryIndex != entry.index) {
                        fromIndex = 0;
                    }
                    startingIndex = 0;
                }
                final int nextIndex = entry.bits.nextSetBit(fromIndex);
                if (nextIndex != -1) {
                    currentEntry = entry;
                    currentEntryBase = entry.index << BITS_PER_ENTRY;
                    next = nextIndex;
                    return true;
                }
            }
            return false;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private final static class ReverseItemIterator<V> implements Iterator<PersistentLongMap.Entry<V>> {
        @NotNull
        private final Iterator<Entry> iterator;
        private Entry currentEntry = null;
        private long currentEntryBase = 0;
        private int next = -1;

        ReverseItemIterator(AbstractPersistent23Tree<Entry> tree) {
            iterator = tree.reverseIterator();
        }

        @SuppressWarnings("unchecked")
        @Override
        public PersistentLongMap.Entry<V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final int index = this.next;
            final long key = index + currentEntryBase;
            final Object result = currentEntry.data[index];
            this.next = currentEntry.bits.previousSetBit(index - 1);
            return new LongMapEntry<>(key, (V) result);
        }

        @Override
        public boolean hasNext() {
            return next != -1 || fetchEntry();
        }

        private boolean fetchEntry() {
            while (iterator.hasNext()) {
                final Entry entry = iterator.next();
                final int prevIndex = entry.bits.length() - 1;
                if (prevIndex != -1) {
                    currentEntry = entry;
                    currentEntryBase = entry.index << BITS_PER_ENTRY;
                    next = prevIndex;
                    return true;
                }
            }
            return false;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private final static class ReverseItemTailIterator<V> implements Iterator<PersistentLongMap.Entry<V>> {
        @NotNull
        private final Iterator<Entry> iterator;
        private final long finishingEntryIndex;
        private final int finishingIndex;

        private Entry currentEntry = null;
        private long currentEntryBase = 0;
        private int next = -1;

        ReverseItemTailIterator(AbstractPersistent23Tree<Entry> tree, long minKey) {
            finishingEntryIndex = getEntryIndex(minKey);
            this.iterator = tree.tailReverseIterator(makeIndexEntry(finishingEntryIndex));
            this.finishingIndex = (int) (minKey & MASK);
        }

        @SuppressWarnings("unchecked")
        @Override
        public PersistentLongMap.Entry<V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final int index = this.next;
            final long key = index + currentEntryBase;
            final Entry entry = this.currentEntry;
            final Object result = entry.data[index];
            final int prevIndex = entry.bits.previousSetBit(index - 1);
            if (entry.index == finishingEntryIndex && prevIndex < finishingIndex) {
                this.next = -1;
            } else {
                this.next = prevIndex;
            }
            return new LongMapEntry<>(key, (V) result);
        }

        @Override
        public boolean hasNext() {
            return next != -1 || fetchEntry();
        }

        private boolean fetchEntry() {
            while (iterator.hasNext()) {
                final Entry entry = iterator.next();
                if (entry.index < finishingEntryIndex) {
                    return false;
                }
                final int prevIndex = entry.bits.length() - 1;
                if (entry.index == finishingEntryIndex && prevIndex < finishingIndex) {
                    return false;
                }
                if (prevIndex != -1) {
                    currentEntry = entry;
                    currentEntryBase = entry.index << BITS_PER_ENTRY;
                    next = prevIndex;
                    return true;
                }
            }
            return false;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
