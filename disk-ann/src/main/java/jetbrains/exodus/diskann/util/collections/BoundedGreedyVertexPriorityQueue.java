/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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
package jetbrains.exodus.diskann.util.collections;

public final class BoundedGreedyVertexPriorityQueue {
    private static final int CHECKED_FLAG = 1;
    private static final int PQ_DISTANCE_FLAG = 2;

    private static final int PRELOADED_FLAG = 4;

    private final int[] vertices;
    private final float[] distances;

    private final byte[] flags;

    private int nextNotCheckedVertex;
    private int size;

    private final int capacity;

    public BoundedGreedyVertexPriorityQueue(int capacity) {
        this.capacity = capacity;
        vertices = new int[capacity];
        distances = new float[capacity];
        flags = new byte[capacity];
    }

    public int add(int vertexIndex, float distance, boolean pqDistance) {
        var index = binarySearch(distance, 0, size);

        if (size == capacity && index == size) {
            return -1;
        }

        if (nextNotCheckedVertex > index) {
            nextNotCheckedVertex = index;
        }

        if (index < vertices.length - 1) {
            var newIndex = index + 1;
            var endIndex = Math.min(size - index + newIndex, vertices.length);

            System.arraycopy(vertices, index, vertices, newIndex, endIndex - newIndex);
            System.arraycopy(distances, index, distances, newIndex, endIndex - newIndex);
            System.arraycopy(flags, index, flags, newIndex, endIndex - newIndex);
        }


        var flag = pqDistance ? PQ_DISTANCE_FLAG : 0;

        distances[index] = distance;
        vertices[index] = vertexIndex;
        flags[index] = (byte) flag;

        if (size < capacity) {
            size++;
        }

        return index;
    }

    public int peekNextNotCheckedNextVertexIndex() {
        return nextNotCheckedVertex;
    }

    public int nextNotCheckedVertexIndex() {
        if (nextNotCheckedVertex >= size) {
            return -1;
        }

        var result = nextNotCheckedVertex;

        flags[nextNotCheckedVertex] |= CHECKED_FLAG;
        nextNotCheckedVertex++;

        while (nextNotCheckedVertex < vertices.length && (flags[nextNotCheckedVertex] & CHECKED_FLAG) != 0) {
            nextNotCheckedVertex++;
        }

        if (nextNotCheckedVertex >= size) {
            nextNotCheckedVertex = Integer.MAX_VALUE;
        }

        return result;
    }

    public int vertexIndex(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException();
        }

        return vertices[index];
    }

    public float vertexDistance(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException();
        }

        return distances[index];
    }

    public float maxDistance() {
        if (size == 0) {
            return Float.NaN;
        }

        return distances[size - 1];
    }

    public int resortVertex(int index, float newDistance) {
        if (index >= size) {
            throw new IndexOutOfBoundsException();
        }

        var distance = distances[index];

        int newIndex;
        if (newDistance < distance) {
            newIndex = binarySearch(newDistance, 0, index);
        } else if (newDistance > distance) {
            newIndex = binarySearch(newDistance, index + 1, size) - 1;
            assert newIndex >= 0;
        } else {
            flags[index] = 0;

            if (nextNotCheckedVertex > index) {
                nextNotCheckedVertex = index;
            }

            return index;
        }

        assert newIndex < size;

        if (index == newIndex) {
            distances[index] = newDistance;
            flags[index] = 0;

            if (nextNotCheckedVertex > newIndex) {
                nextNotCheckedVertex = newIndex;
            }

            return newIndex;
        }

        var vertexIndex = vertices[index];

        if (index < newIndex) {
            System.arraycopy(vertices, index + 1, vertices, index,
                    newIndex - index);
            System.arraycopy(distances, index + 1, distances, index,
                    newIndex - index);
            System.arraycopy(flags, index + 1, flags, index,
                    newIndex - index);

            if (nextNotCheckedVertex > index && nextNotCheckedVertex <= newIndex) {
                nextNotCheckedVertex--;
            }
        } else {
            System.arraycopy(vertices, newIndex, vertices, newIndex + 1,
                    index - newIndex);
            System.arraycopy(distances, newIndex, distances, newIndex + 1,
                    index - newIndex);
            System.arraycopy(flags, newIndex, flags, newIndex + 1,
                    index - newIndex);

            if (nextNotCheckedVertex >= newIndex && nextNotCheckedVertex < index) {
                nextNotCheckedVertex++;
            }
        }


        distances[newIndex] = newDistance;
        vertices[newIndex] = vertexIndex;
        flags[newIndex] = 0;

        if (nextNotCheckedVertex > newIndex) {
            nextNotCheckedVertex = newIndex;
        }

        return newIndex;
    }

    public boolean isPqDistance(int index) {
        return (flags[index] & PQ_DISTANCE_FLAG) != 0;
    }

    public int size() {
        return size;
    }

    public void vertexIndices(long[] result, int maxResultSize) {
        var resultSize = Math.min(size, maxResultSize);
        for (int i = 0; i < resultSize; i++) {
            result[i] = vertices[i];
        }
    }

    public void clear() {
        size = 0;
        nextNotCheckedVertex = 0;
    }

    public void fetchNotCheckedNotPreloaded(int[] vertexIndexes, int from, int size, int[] resultSizeAndLastIndex) {
        var resultSize = 0;
        size = Math.min(size, vertexIndexes.length);

        var lastIndex = -1;
        for (int i = from; i < this.size && resultSize < size; i++) {
            var flag = flags[i];

            if ((flag & (CHECKED_FLAG | PRELOADED_FLAG)) == 0) {
                vertexIndexes[resultSize++] = vertices[i];
                flags[i] = (byte) (flag | PRELOADED_FLAG);
                lastIndex = i;
            }
        }

        resultSizeAndLastIndex[0] = resultSize;
        resultSizeAndLastIndex[1] = lastIndex;
    }

    private int binarySearch(float distance, int form, int to) {
        int start = form;
        int end = to - 1;

        int mid = (end + start) >>> 1;

        while (start <= end) {
            var midDistance = distances[mid];
            if (midDistance == distance) {
                return mid;
            } else if (midDistance < distance) {
                start = mid + 1;
            } else {
                end = mid - 1;
            }

            mid = (end + start) / 2;
        }

        return start;
    }
}
