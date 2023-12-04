/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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
package jetbrains.exodus.tree.patricia;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Iterator;

final class ChildReferenceSet implements Iterable<ChildReference> {

    private ChildReference[] refs;

    ChildReferenceSet() {
        clear(0);
    }

    void clear(final int capacity) {
        refs = capacity == 0 ? null : new ChildReference[capacity];
    }

    int size() {
        return refs == null ? 0 : refs.length;
    }

    boolean isEmpty() {
        return size() == 0;
    }

    ChildReference get(final byte b) {
        final int index = searchFor(b);
        return index < 0 ? null : refs[index];
    }

    ChildReference getRight() {
        final int size = size();
        return size > 0 ? refs[size - 1] : null;
    }

    int searchFor(final byte b) {
        final ChildReference[] refs = this.refs;
        final int key = b & 0xff;
        int low = 0;
        int high = size() - 1;
        while (low <= high) {
            int mid = (low + high + 1) >>> 1;
            final ChildReference midRef = refs[mid];
            final int cmp = midRef == null ? 1 : (midRef.firstByte & 0xff) - key;
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -low - 1;
    }

    ChildReference referenceAt(final int index) {
        return refs[index];
    }

    void putRight(@NotNull final ChildReference ref) {
        final int size = this.size();
        ensureCapacity(size + 1, size);
        refs[size] = ref;
    }

    void insertAt(final int index, @NotNull final ChildReferenceMutable ref) {
        ensureCapacity(this.size() + 1, index);
        refs[index] = ref;
    }

    void setAt(final int index, @NotNull final ChildReference ref) {
        refs[index] = ref;
    }

    boolean remove(final byte b) {
        final int index = searchFor(b);
        if (index < 0) {
            return false;
        }
        final int size = this.size();
        if (size == 1) {
            refs = null;
        } else {
            final ChildReference[] refs = this.refs;
            final int refsToCopy = size - index - 1;
            if (refsToCopy > 0) {
                System.arraycopy(refs, index + 1, refs, index, refsToCopy);
            }
            this.refs = Arrays.copyOf(refs, refs.length - 1);
        }
        return true;
    }

    @Override
    public ChildReferenceIterator iterator() {
        return iterator(-1);
    }

    ChildReferenceIterator iterator(final int index) {
        return new ChildReferenceIterator(this, index);
    }

    private void ensureCapacity(final int capacity, final int insertPos) {
        final ChildReference[] refs = this.refs;
        if (refs == null) {
            this.refs = new ChildReference[capacity];
        } else {
            final int length = refs.length;
            if (length >= capacity) {
                if (insertPos < length - 1) {
                    System.arraycopy(refs, insertPos, refs, insertPos + 1, length - insertPos - 1);
                }
            } else {
                this.refs = new ChildReference[Math.max(length, capacity)];
                System.arraycopy(refs, 0, this.refs, 0, insertPos);
                System.arraycopy(refs, insertPos, this.refs, insertPos + 1, length - insertPos);
                // refs[insertPos] == null
            }
        }
    }

    static final class ChildReferenceIterator implements Iterator<ChildReference> {

        private final ChildReference[] refs;
        private final int size;
        private int index;

        ChildReferenceIterator(ChildReferenceSet set, int index) {
            refs = set.refs;
            size = set.size();
            this.index = index;
        }

        @Override
        public boolean hasNext() {
            return index < size - 1;
        }

        @Override
        public ChildReference next() {
            ChildReference ref;
            int i = index;
            do {
                if (++i >= size) {
                    return null;
                }
                ref = refs[i];
            } while (ref == null);
            index = i;
            return ref;
        }

        public ChildReference prev() {
            ChildReference ref;
            int i = index;
            do {
                if (--i < 0) {
                    return null;
                }
                ref = refs[i];
            } while (ref == null);
            index = i;
            return ref;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        int getIndex() {
            return index;
        }

        @Nullable
        ChildReference currentRef() {
            final int i = this.index;
            return i >= 0 && i < size ? refs[i] : null;
        }
    }
}
