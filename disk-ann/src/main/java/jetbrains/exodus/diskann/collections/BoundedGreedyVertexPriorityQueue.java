package jetbrains.exodus.diskann.collections;

public final class BoundedGreedyVertexPriorityQueue {
    //Flattened presentation of a sorted array of vertex objects
    //1. Distance - float type presented as integer
    //2. Vertex index - int type
    //3. Is distance of vertex calculated by PQ and whether it is visited- two bits stored into int type

    private final int[] vertices;
    private int nextNotCheckedVertex;
    private int size;

    private final int capacity;

    public BoundedGreedyVertexPriorityQueue(int capacity) {
        this.capacity = capacity;
        vertices = new int[capacity * 3];
    }

    public void add(int vertexIndex, float distance, boolean pqDistance) {
        var index = binarySearch(distance, 0, size);
        if (size == capacity && index == size) {
            return;
        }

        if (nextNotCheckedVertex > index) {
            nextNotCheckedVertex = index;
        }

        var arrayIndex = arrayIndex(index);
        if (arrayIndex < vertices.length - 3) {
            var newArrayIndex = arrayIndex + 3;
            var endIndex = Math.min(arrayIndex(size) - arrayIndex + newArrayIndex, vertices.length);
            System.arraycopy(vertices, arrayIndex, vertices, newArrayIndex, endIndex - newArrayIndex);
        }


        var intDistance = Float.floatToIntBits(distance);
        var intPqDistance = pqDistance ? 2 : 0;

        vertices[arrayIndex] = intDistance;
        vertices[arrayIndex + 1] = vertexIndex;
        vertices[arrayIndex + 2] = intPqDistance;

        if (size < capacity) {
            size++;
        }
    }

    public int nextNotCheckedVertexIndex() {
        if (nextNotCheckedVertex >= size) {
            return -1;
        }

        var result = nextNotCheckedVertex;
        var arrayIndex = arrayIndex(nextNotCheckedVertex);
        vertices[arrayIndex + 2] |= 1;

        arrayIndex += 3;
        nextNotCheckedVertex++;

        while (arrayIndex < vertices.length && (vertices[arrayIndex + 2] & 1) != 0) {
            arrayIndex += 3;
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

        var arrayIndex = arrayIndex(index);
        return vertices[arrayIndex + 1];
    }

    public float vertexDistance(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException();
        }

        var arrayIndex = arrayIndex(index);
        return Float.intBitsToFloat(vertices[arrayIndex]);
    }

    public float maxDistance() {
        if (size == 0) {
            return Float.NaN;
        }

        var arrayIndex = arrayIndex(size - 1);
        return Float.intBitsToFloat(vertices[arrayIndex]);
    }

    public void resortVertex(int index, float newDistance) {
        if (index >= size) {
            throw new IndexOutOfBoundsException();
        }

        var arrayIndex = arrayIndex(index);

        var distance = Float.intBitsToFloat(vertices[arrayIndex]);

        int newIndex;
        if (newDistance < distance) {
            newIndex = binarySearch(newDistance, 0, index);
        } else if (newDistance > distance) {
            newIndex = binarySearch(newDistance, index + 1, size) - 1;
            assert newIndex >= 0;
        } else {
            vertices[arrayIndex + 2] = 0;

            if (nextNotCheckedVertex > index) {
                nextNotCheckedVertex = index;
            }

            return;
        }

        assert newIndex < size;

        var newIntDistance = Float.floatToIntBits(newDistance);

        if (index == newIndex) {
            vertices[arrayIndex] = newIntDistance;
            vertices[arrayIndex + 2] = 0;

            if (nextNotCheckedVertex > newIndex) {
                nextNotCheckedVertex = newIndex;
            }

            return;
        }

        var vertexIndex = vertices[arrayIndex + 1];

        var newArrayIndex = arrayIndex(newIndex);
        if (index < newIndex) {
            System.arraycopy(vertices, arrayIndex + 3, vertices, arrayIndex,
                    newArrayIndex - arrayIndex);
            if (nextNotCheckedVertex > index && nextNotCheckedVertex <= newIndex) {
                nextNotCheckedVertex--;
            }
        } else {
            System.arraycopy(vertices, newArrayIndex, vertices, newArrayIndex + 3,
                    arrayIndex - newArrayIndex);
            if (nextNotCheckedVertex >= newIndex && nextNotCheckedVertex < index) {
                nextNotCheckedVertex++;
            }
        }


        vertices[newArrayIndex] = newIntDistance;
        vertices[newArrayIndex + 1] = vertexIndex;
        vertices[newArrayIndex + 2] = 0;

        if (nextNotCheckedVertex > newIndex) {
            nextNotCheckedVertex = newIndex;
        }
    }

    public boolean isPqDistance(int index) {
        var arrayIndex = arrayIndex(index);
        return (vertices[arrayIndex + 2] & 2) != 0;
    }

    public int size() {
        return size;
    }

    public void vertexIndices(long[] result, int maxResultSize) {
        var resultSize = Math.min(size, maxResultSize);
        for (int i = 0, arrayIndex = 1; i < resultSize; i++, arrayIndex += 3) {
            result[i] = vertices[arrayIndex];
        }
    }

    public void clear() {
        size = 0;
        nextNotCheckedVertex = 0;
    }

    private int binarySearch(double distance, int form, int to) {
        int start = form;
        int end = to - 1;

        int mid = (end + start) >>> 1;

        while (start <= end) {
            var arrayIndex = arrayIndex(mid);
            var midDistance = Float.intBitsToFloat(vertices[arrayIndex]);
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

    private int arrayIndex(int index) {
        return index * 3;
    }

}
