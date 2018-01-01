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
package jetbrains.exodus.core.dataStructures;

public class LongArrayList implements Cloneable {

    private long[] data;
    private int size;

    public LongArrayList(final int initialCapacity) {
        data = new long[initialCapacity];
    }

    public LongArrayList() {
        this(4);
    }

    public void trimToSize() {
        final int oldCapacity = data.length;
        if (size < oldCapacity) {
            final long[] oldData = data;
            data = new long[size];
            System.arraycopy(oldData, 0, data, 0, size);
        }
    }

    public void ensureCapacity(final int minCapacity) {
        int oldCapacity = data.length;
        if (minCapacity > oldCapacity) {
            if (oldCapacity == 0) {
                oldCapacity = 1;
            }
            int newCapacity = (oldCapacity << 3) / 5 + 1;
            if (newCapacity < minCapacity) {
                newCapacity = minCapacity;
            }
            final long[] oldData = data;
            data = new long[newCapacity];
            System.arraycopy(oldData, 0, data, 0, size);
        }
    }

    public int getCapacity() {
        return data.length;
    }

    public void setCapacity(int capacity) {
        data = new long[capacity];
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public boolean contains(final long element) {
        return indexOf(element) >= 0;
    }

    public int indexOf(final long element) {
        return indexOf(data, size, element);
    }

    public static int indexOf(long[] array, long element) {
        return indexOf(array, array.length, element);
    }

    public static int indexOf(long[] array, int size, long element) {
        for (int i = 0; i < size; i++) {
            if (element == array[i]) return i;
        }
        return -1;
    }

    public int lastIndexOf(final long element) {
        for (int i = size - 1; i >= 0; i--) {
            if (element == data[i]) return i;
        }
        return -1;
    }

    @Override
    public final Object clone() throws CloneNotSupportedException {
        final LongArrayList v = (LongArrayList) super.clone();
        v.data = new long[size];
        System.arraycopy(data, 0, v.data, 0, size);
        return v;
    }

    public long[] toArray() {
        final long[] result = new long[size];
        System.arraycopy(data, 0, result, 0, size);
        return result;
    }

    public long[] toArray(long[] a) {
        if (a.length < size) {
            a = new long[size];
        }
        System.arraycopy(data, 0, a, 0, size);
        return a;
    }

    public long[] getInstantArray() {
        return data;
    }

    public long get(final int index) {
        checkRange(index);
        return data[index];
    }

    public long set(final int index, final long element) {
        checkRange(index);

        final long oldValue = data[index];
        data[index] = element;
        return oldValue;
    }

    public void add(final long element) {
        ensureCapacity(size + 1);
        data[size++] = element;
    }

    public void add(final int index, final long element) {
        if (index > size || index < 0) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }

        ensureCapacity(size + 1);
        System.arraycopy(data, index, data, index + 1, size - index);
        data[index] = element;
        size++;
    }

    public long remove(final int index) {
        checkRange(index);

        final long oldValue = data[index];

        final int numMoved = size - index - 1;
        if (numMoved > 0) {
            System.arraycopy(data, index + 1, data, index, numMoved);
        }
        size--;

        return oldValue;
    }

    public void clear() {
        size = 0;
    }

    protected void removeRange(final int fromIndex, final int toIndex) {
        final int numMoved = size - toIndex;
        System.arraycopy(data, toIndex, data, fromIndex, numMoved);
        size -= (toIndex - fromIndex);
    }

    private void checkRange(final int index) {
        if (index >= size || index < 0) {
            //noinspection HardCodedStringLiteral
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
    }
}
