/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.core.dataStructures.persistent.trial;

import java.util.Iterator;

public class PersistentList<T> implements Iterable<T> {

    private static final Object[] WHATEVER = new Object[32];
    protected static final int MASK_31 = 0x01F;

    public static final PersistentList EMPTY = new PersistentList(0, 5, WHATEVER, new Object[0]);

    protected int size;
    protected int shift;
    protected Object[] root;
    protected T[] recent;

    @SuppressWarnings({"AssignmentToCollectionOrArrayFieldFromParameter"})
    public PersistentList(int size, int shift, Object[] root, T[] recent) {
        this.size = size;
        this.shift = shift;
        this.root = root;
        this.recent = recent;
    }

    @Override
    public Iterator<T> iterator() {
        return new SequenceIterable.Iterator<>(asSequence());
    }

    public SequenceImpl<T> asSequence() {
        return size == 0 ? null : new SequenceImpl<>(this, 0, 0);
    }

    int recentOffset() {
        if (size < 32) {
            return 0;
        }
        return size - 1 >>> 5 << 5;
    }

    public T[] arrayFor(int i) {
        if (i >= 0 && i < size) {
            if (i >= recentOffset()) {
                return recent;
            }
            Object[] node = root;
            int level = shift;
            while (true) {
                final Object child = node[i >>> level & MASK_31];
                level -= 5;
                if (level <= 0) {
                    return (T[]) child;
                }
                node = (Object[]) child;
            }
        }
        throw new IndexOutOfBoundsException();
    }

    public T get(int i) {
        Object[] node = arrayFor(i);
        return (T) node[i & MASK_31];
    }

    static Object[] newPath(int level, Object[] node) {
        if (level == 0) {
            return node;
        }
        final Object[] result = new Object[32];
        result[0] = newPath(level - 5, node);
        return result;
    }

    private Object[] pushRecent(int level, Object[] parent, T[] recent_) {
        final int subIndex = size - 1 >>> level & MASK_31;
        final Object[] result = ensureMutable(parent);
        final Object[] target;
        if (level == 5) {
            target = recent_;
        } else {
            Object child = parent[subIndex];
            target = child != null ?
                    pushRecent(level - 5, (Object[]) child, recent_)
                    : newPath(level - 5, recent_);
        }
        result[subIndex] = target;
        return result;
    }

    protected Object[] ensureMutable(Object[] parent) {
        return parent.clone();
    }

    public PersistentList<T> add(final T item) {
        if (size - recentOffset() < 32) {
            T[] newRecent = (T[]) new Object[recent.length + 1];
            System.arraycopy(recent, 0, newRecent, 0, recent.length);
            newRecent[recent.length] = item;
            return mutableCopy(size + 1, shift, root, newRecent);
        }
        Object[] newRoot;
        int newShift = shift;
        if (size >>> 5 > 1 << shift) {
            newRoot = new Object[32];
            newRoot[0] = root;
            newRoot[1] = newPath(shift, recent);
            newShift += 5;
        } else {
            newRoot = pushRecent(shift, root, recent);
        }
        return mutableCopy(size + 1, newShift, newRoot, (T[]) new Object[]{item});
    }

    protected PersistentList<T> mutableCopy(int newSize, int newShift, Object[] newRoot, T[] newRecent) {
        return new PersistentList<>(newSize, newShift, newRoot, newRecent);
    }

    public PersistentList<T> pop() {
        if (size == 0) {
            throw new IllegalStateException(); // TODO: sure?
        }
        if (size == 1) {
            return maybeEmpty();
        }
        if (size - recentOffset() > 1) {
            final T[] newRecent = (T[]) new Object[recent.length - 1];
            System.arraycopy(recent, 0, newRecent, 0, newRecent.length);
            return mutableCopy(size - 1, shift, root, newRecent);
        }
        T[] newRecent = arrayFor(size - 2);

        Object[] newRoot = popRecent(shift, root);
        int newShift = shift;
        if (newRoot == null) {
            newRoot = WHATEVER;
        }
        if (shift > 5 && newRoot[1] == null) {
            newRoot = (Object[]) newRoot[0];
            newShift -= 5;
        }
        return mutableCopy(size - 1, newShift, newRoot, newRecent);
    }

    protected PersistentList<T> maybeEmpty() {
        return EMPTY;
    }

    public MutablePersistentList<T> toMutable() {
        return new MutablePersistentList<>(this);
    }

    private Object[] popRecent(int level, Object[] node) {
        final int subIndex = size - 2 >>> level & MASK_31;
        if (level > 5) {
            Object[] newChild = popRecent(level - 5, (Object[]) node[subIndex]);
            if (newChild == null && subIndex == 0) {
                return null;
            } else {
                final Object[] result = ensureMutable(node);
                result[subIndex] = newChild;
                return result;
            }
        } else if (subIndex == 0) {
            return null;
        } else {
            final Object[] result = ensureMutable(node);
            result[subIndex] = null;
            return result;
        }
    }

    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    public static final class SequenceImpl<T> implements Sequence<T> {

        private final PersistentList<T> source;
        private final T[] node;
        private final int i;
        private final int offset;

        public SequenceImpl(PersistentList<T> source, int i, int offset) {
            this.source = source;
            this.i = i;
            this.offset = offset;
            this.node = source.arrayFor(i);
        }

        SequenceImpl(PersistentList<T> source, T[] node, int i, int offset) {
            this.source = source;
            this.i = i;
            this.offset = offset;
            this.node = node;
        }

        @Override
        public T first() {
            return node[offset];
        }

        @Override
        public Sequence<T> skip() {
            if (offset + 1 < node.length) {
                return new SequenceImpl<>(source, node, i, offset + 1);
            }
            if (i + node.length < source.size) {
                return new SequenceImpl<>(source, i + node.length, 0);
            }
            return null;
        }
    }

    public static void main(String[] args) {
        PersistentList<String> l = PersistentList.EMPTY;
        for (int i = 0; i < 9000; i++) {
            l = l.add("test " + i);
        }
        int count = 0;
        for (final String s : l) {
            if (!("test " + count).equals(s)) {
                System.out.println("boom");
            }
            count++;
        }
        for (int i = 0; i < 9000; i++) {
            l = l.pop();
        }
        System.out.println(l.size);
        l.iterator().hasNext();
    }

}
