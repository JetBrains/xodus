/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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

import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import org.jetbrains.annotations.NotNull;

import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class PersistentBitTreeLongSet implements PersistentLongSet {
    private static final int BITS_PER_ENTRY = 10;
    private static final int ELEMENTS_PER_ENTRY = 1 << BITS_PER_ENTRY;
    private static final int MASK = ELEMENTS_PER_ENTRY - 1;
    private static final BitSet FAKE = new BitSet();

    @NotNull
    private final Root root;

    public PersistentBitTreeLongSet() {
        this.root = new Root(new Persistent23Tree<Entry>(), 0);
    }

    private PersistentBitTreeLongSet(@NotNull final Root root) {
        this.root = root;
    }

    @Override
    public PersistentLongSet.ImmutableSet beginRead() {
        return new ImmutableSet(root);
    }

    @Override
    public PersistentLongSet getClone() {
        return new PersistentBitTreeLongSet(root.getClone());
    }

    @Override
    public PersistentLongSet.MutableSet beginWrite() {
        return new MutableSet(root.map.beginWrite(), root.size, this);
    }

    private static long getEntryIndex(long value) {
        return value >> BITS_PER_ENTRY;
    }

    protected static class ImmutableSet implements PersistentLongSet.ImmutableSet {
        @NotNull
        protected final Root root;

        ImmutableSet(@NotNull final Root root) {
            this.root = root;
        }

        @Override
        public boolean contains(long key) {
            final long index = getEntryIndex(key);
            final Entry entry = root.map.getRoot().get(new Entry(index, null));
            return entry != null && entry.bits.get((int) (key & MASK));
        }

        @Override
        public LongIterator longIterator() {
            return new ItemIterator(root.map);
        }

        @Override
        public boolean isEmpty() {
            return root.size == 0;
        }

        @Override
        public int size() {
            return root.size;
        }
    }

    protected static class MutableSet implements PersistentLongSet.MutableSet {
        @NotNull
        private final Persistent23Tree.MutableTree<Entry> mutableSet;
        private int size;
        private final PersistentBitTreeLongSet baseSet;

        MutableSet(@NotNull Persistent23Tree.MutableTree<Entry> mutableSet, int size, PersistentBitTreeLongSet baseSet) {
            this.mutableSet = mutableSet;
            this.size = size;
            this.baseSet = baseSet;
        }

        @Override
        public boolean contains(long key) {
            final long index = getEntryIndex(key);
            final AbstractPersistent23Tree.RootNode<Entry> root = mutableSet.getRoot();
            if (root == null) {
                return false;
            }
            final Entry entry = root.get(new Entry(index, null));
            if (entry == null) {
                return false;
            }
            int bitIndex = (int) (key & MASK);
            return entry.bits.get(bitIndex);
        }

        @Override
        public LongIterator longIterator() {
            return new ItemIterator(mutableSet);
        }

        @Override
        public boolean isEmpty() {
            return size == 0;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public void add(long key) {
            final long index = getEntryIndex(key);
            Entry entry;
            final AbstractPersistent23Tree.RootNode<Entry> root = mutableSet.getRoot();
            if (root == null) {
                entry = null;
            } else {
                entry = mutableSet.getRoot().get(new Entry(index, null));
            }
            int bitIndex = (int) (key & MASK);
            if (entry == null) {
                entry = new Entry(index);
                mutableSet.add(entry);
                size++;
            } else {
                if (!entry.bits.get(bitIndex)) {
                    final Entry copy = new Entry(index, entry);
                    mutableSet.add(copy);
                    entry = copy;
                    size++;
                }
            }
            entry.bits.set(bitIndex);
        }

        @Override
        public boolean remove(long key) {
            final long index = getEntryIndex(key);
            final AbstractPersistent23Tree.RootNode<Entry> root = mutableSet.getRoot();
            if (root == null) {
                return false;
            }
            Entry entry = mutableSet.getRoot().get(new Entry(index, null));
            if (entry == null) {
                return false;
            }
            int bitIndex = (int) (key & MASK);
            if (entry.bits.get(bitIndex)) {
                size--;
            } else {
                return false;
            }
            final Entry copy = new Entry(index, entry);
            copy.bits.clear(bitIndex);
            if (copy.bits.isEmpty()) {
                mutableSet.exclude(entry);
            } else {
                mutableSet.add(copy);
            }
            return true;
        }

        @Override
        public boolean endWrite() {
            if (!mutableSet.endWrite()) {
                return false;
            }
            // TODO: consistent size update
            baseSet.root.size = size;
            return true;
        }
    }

    protected static class Entry implements Comparable<Entry> {
        private final long index;
        @NotNull
        private final BitSet bits;

        public Entry(long min) {
            this.index = min;
            this.bits = new BitSet(ELEMENTS_PER_ENTRY);
        }

        public Entry(long min, Entry other) {
            this.index = min;
            if (other != null) {
                this.bits = new BitSet(ELEMENTS_PER_ENTRY);
                this.bits.or(other.bits);
            } else {
                this.bits = FAKE; // querying entry must have no bits in order to reduce memory and GC pressure
            }
        }

        @Override
        public int compareTo(@NotNull PersistentBitTreeLongSet.Entry o) {
            final long otherMin = o.index;
            return index > otherMin ? 1 : index == otherMin ? 0 : -1;
        }
    }

    protected static class Root {

        @NotNull
        private final Persistent23Tree<Entry> map;
        private int size;

        Root(@NotNull Persistent23Tree<Entry> map, int size) {
            this.map = map;
            this.size = size;
        }

        public Root getClone() {
            return new Root(map.getClone(), size);
        }
    }

    protected static class ItemIterator implements LongIterator {
        @NotNull
        protected final Iterator<Entry> iterator;
        protected Entry currentEntry = null;
        protected long currentEntryBase = 0;
        protected int next = -1;

        ItemIterator(AbstractPersistent23Tree<Entry> tree) {
            iterator = tree.iterator();
        }

        @Override
        public Long next() {
            return nextLong();
        }

        @Override
        public long nextLong() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final int index = this.next;
            final long result = index + currentEntryBase;
            this.next = currentEntry.bits.nextSetBit(index + 1);
            return result;
        }

        @Override
        public boolean hasNext() {
            return next != -1 || fetchEntry() != -1;
        }

        public int fetchEntry() {
            while (iterator.hasNext()) {
                final Entry entry = iterator.next();
                final int nextIndex = entry.bits.nextSetBit(0);
                if (nextIndex != -1) {
                    currentEntry = entry;
                    currentEntryBase = entry.index << BITS_PER_ENTRY;
                    next = nextIndex;
                    return nextIndex;
                }
            }
            return -1;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
