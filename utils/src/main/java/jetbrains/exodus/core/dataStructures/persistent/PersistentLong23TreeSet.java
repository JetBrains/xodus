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

import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class PersistentLong23TreeSet implements PersistentLongSet {

    private final Persistent23Tree<PersistentLongMap.Entry<Boolean>> set;

    public PersistentLong23TreeSet() {
        this(null);
    }

    private PersistentLong23TreeSet(@Nullable final AbstractPersistent23Tree.RootNode<PersistentLongMap.Entry<Boolean>> root) {
        set = new Persistent23Tree<>(root);
    }

    @Override
    public PersistentLongSet.ImmutableSet beginRead() {
        return new ImmutableSet(set.getRoot());
    }

    @Override
    public PersistentLong23TreeSet getClone() {
        return new PersistentLong23TreeSet(set.getRoot());
    }

    @Override
    public PersistentLongSet.MutableSet beginWrite() {
        return new MutableSet(set);
    }

    protected static class ImmutableSet extends PersistentLong23TreeMap.ImmutableMap<Boolean> implements PersistentLongSet.ImmutableSet {

        ImmutableSet(RootNode<PersistentLongMap.Entry<Boolean>> root) {
            super(root);
        }

        @Override
        public boolean contains(long key) {
            return containsKey(key);
        }

        @Override
        public LongIterator longIterator() {
            return new IteratorImpl(iterator());
        }

        @Override
        public LongIterator reverseLongIterator() {
            return new IteratorImpl(reverseIterator());
        }

        @Override
        public LongIterator tailLongIterator(long key) {
            return new IteratorImpl(tailEntryIterator(key));
        }

        @Override
        public LongIterator tailReverseLongIterator(long key) {
            return new IteratorImpl(tailReverseEntryIterator(key));
        }
    }

    protected static class MutableSet implements PersistentLongSet.MutableSet {

        private final PersistentLong23TreeMap.MutableMap<Boolean> map;

        MutableSet(Persistent23Tree<PersistentLongMap.Entry<Boolean>> set) {
            map = new PersistentLong23TreeMap.MutableMap<>(set);
        }

        @Override
        public LongIterator longIterator() {
            return new IteratorImpl(map.iterator());
        }

        @Override
        public LongIterator reverseLongIterator() {
            return new IteratorImpl(map.reverseIterator());
        }

        @Override
        public LongIterator tailLongIterator(long key) {
            return new IteratorImpl(map.tailEntryIterator(key));
        }

        @Override
        public LongIterator tailReverseLongIterator(long key) {
            return new IteratorImpl(map.tailReverseEntryIterator(key));
        }

        @Override
        public boolean isEmpty() {
            return map.isEmpty();
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public boolean contains(long key) {
            return map.get(key) == Boolean.TRUE;
        }

        @Override
        public void add(long key) {
            map.put(key, Boolean.TRUE);
        }

        @Override
        public boolean remove(long key) {
            Boolean result = map.remove(key);
            if (result == null) {
                return false;
            }
            return result;
        }

        @Override
        public void clear() {
            map.setRoot(null);
        }

        @Override
        public boolean endWrite() {
            return map.endWrite();
        }
    }

    protected static class IteratorImpl implements LongIterator {
        private final Iterator<PersistentLongMap.Entry<Boolean>> it;

        IteratorImpl(Iterator<PersistentLongMap.Entry<Boolean>> it) {
            this.it = it;
        }

        @Override
        public long nextLong() {
            PersistentLongMap.Entry<Boolean> entry = it.next();
            if (entry == null) {
                throw new NoSuchElementException();
            }
            return entry.getKey();
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public Long next() {
            PersistentLongMap.Entry<Boolean> entry = it.next();
            if (entry == null) {
                throw new NoSuchElementException();
            }
            return entry.getKey();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
