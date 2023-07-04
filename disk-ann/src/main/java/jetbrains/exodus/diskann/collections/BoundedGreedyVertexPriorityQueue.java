package jetbrains.exodus.diskann.collections;

public final class BoundedGreedyVertexPriorityQueue {
    //Flattened presentation of a sorted array of vertex objects
    //1. Distance - double type presented as two integers
    //2. Vertex index - int type
    //3. Is distance of vertex calculated by PQ and whether it is visited- two bits stored into int type

    private final int[] vertices;
    private int nextNotCheckedVertex;
    private int size;

    public BoundedGreedyVertexPriorityQueue(int capacity) {
        vertices = new int[capacity * 4];
    }

    public void add(int vertexIndex, double distance, boolean pqDistance) {

        var index = binarySearch(distance, 0, size);
        if (size == vertices.length >>> 2 && index == size) {
            return;
        }

        if (nextNotCheckedVertex > index) {
            nextNotCheckedVertex = index;
        }

        var arrayIndex = arrayIndex(index);
        if (arrayIndex < vertices.length - 4) {
            var newArrayIndex = arrayIndex + 4;
            var endIndex = Math.min(arrayIndex(size) - arrayIndex + newArrayIndex, vertices.length);
            System.arraycopy(vertices, arrayIndex, vertices, newArrayIndex, endIndex - newArrayIndex);
        }


        var longDistance = Double.doubleToLongBits(distance);
        var intDistance1 = (int) (longDistance >> 32);
        var intDistance2 = (int) longDistance;
        var intPqDistance = pqDistance ? 2 : 0;

        vertices[arrayIndex] = intDistance1;
        vertices[arrayIndex + 1] = intDistance2;
        vertices[arrayIndex + 2] = vertexIndex;
        vertices[arrayIndex + 3] = intPqDistance;

        if (size < vertices.length >>> 2) {
            size++;
        }
    }

    public int nextNotCheckedVertexIndex() {
        if (nextNotCheckedVertex >= size) {
            return -1;
        }

        var result = nextNotCheckedVertex;
        var arrayIndex = arrayIndex(nextNotCheckedVertex);
        vertices[arrayIndex + 3] |= 1;

        arrayIndex += 4;
        nextNotCheckedVertex++;

        while (arrayIndex < 4 * size && (vertices[arrayIndex + 3] & 1) != 0) {
            arrayIndex += 4;
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
        return vertices[arrayIndex + 2];
    }

    public double vertexDistance(int index) {
        if (index >= size) {
            throw new IndexOutOfBoundsException();
        }

        var arrayIndex = arrayIndex(index);
        var intDistance1 = vertices[arrayIndex];
        var intDistance2 = vertices[arrayIndex + 1];
        var longDistance = ((long) intDistance1 << 32) | (intDistance2 & 0xFFFFFFFFL);

        return Double.longBitsToDouble(longDistance);
    }

    public double maxDistance() {
        if (size == 0) {
            return Double.NaN;
        }

        var arrayIndex = arrayIndex(size - 1);
        var intDistance1 = vertices[arrayIndex];
        var intDistance2 = vertices[arrayIndex + 1];
        var longDistance = ((long) intDistance1 << 32) | (intDistance2 & 0xFFFFFFFFL);

        return Double.longBitsToDouble(longDistance);
    }

    public void resortVertex(int index, double newDistance) {
        if (index >= size) {
            throw new IndexOutOfBoundsException();
        }

        var arrayIndex = arrayIndex(index);
        var intDistance1 = vertices[arrayIndex];
        var intDistance2 = vertices[arrayIndex + 1];

        var distance = Double.longBitsToDouble(((long) intDistance1 << 32) | (intDistance2 & 0xFFFFFFFFL));

        int newIndex;
        if (newDistance < distance) {
            newIndex = binarySearch(newDistance, 0, index);
        } else if (newDistance > distance) {
            newIndex = binarySearch(newDistance, index + 1, size) - 1;
            assert newIndex >= 0;
        } else {
            return;
        }

        assert newIndex < size;

        var newLongDistance = Double.doubleToLongBits(newDistance);
        var newIntDistance1 = (int) (newLongDistance >> 32);
        var newIntDistance2 = (int) newLongDistance;

        if (index == newIndex) {
            vertices[arrayIndex] = newIntDistance1;
            vertices[arrayIndex + 1] = newIntDistance2;
            vertices[arrayIndex + 3] = 0;

            return;
        }

        var vertexIndex = vertices[arrayIndex + 2];

        var newArrayIndex = arrayIndex(newIndex);
        if (index < newIndex) {
            System.arraycopy(vertices, arrayIndex + 4, vertices, arrayIndex,
                    newArrayIndex - arrayIndex);
        } else {
            System.arraycopy(vertices, newArrayIndex, vertices, newArrayIndex + 4,
                    arrayIndex - newArrayIndex);
        }


        vertices[newArrayIndex] = newIntDistance1;
        vertices[newArrayIndex + 1] = newIntDistance2;
        vertices[newArrayIndex + 2] = vertexIndex;
        vertices[newArrayIndex + 3] = 0;

        if (nextNotCheckedVertex > newIndex) {
            nextNotCheckedVertex = newIndex;
        }
    }

    public boolean isPqDistance(int index) {
        var arrayIndex = arrayIndex(index);
        return (vertices[arrayIndex + 3] & 2) != 0;
    }

    public int size() {
        return size;
    }

    public int[] vertexIndices(int maxResultSize) {
        var result = new int[Math.min(size, maxResultSize)];
        for (int i = 0, arrayIndex = 2; i < result.length; i++, arrayIndex += 4) {
            result[i] = vertices[arrayIndex];
        }

        return result;
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
            var midDistance = Double.longBitsToDouble(((long) vertices[arrayIndex]) << 32
                    | (vertices[arrayIndex + 1] & 0xFFFFFFFFL));
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
        return index << 2;
    }

}
