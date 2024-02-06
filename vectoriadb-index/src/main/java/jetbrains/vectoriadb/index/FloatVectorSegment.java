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
package jetbrains.vectoriadb.index;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

class FloatVectorSegment {
    private final int dimensions;
    private final int count;
    private final float[] vectors;

    private FloatVectorSegment(int count, int dimensions, float[] vectors) {
        this.dimensions = dimensions;
        this.count = count;
        this.vectors = vectors;
    }

    public int dimensions() {
        return dimensions;
    }

    public int count() {
        return count;
    }

    public void add(int idx1, FloatVectorSegment v2, int idx2) {
        VectorOperations.add(vectors, idx1 * dimensions, v2.vectors, idx2 * dimensions, vectors, idx1 * dimensions, dimensions);
    }

    public void add(int idx1, MemorySegment v2, int idx2) {
        VectorOperations.add(vectors, idx1 * dimensions, v2, (long) idx2 * dimensions, vectors, idx1 * dimensions, dimensions);
    }

    public void div(int idx, float scalar) {
        VectorOperations.div(vectors, idx * dimensions, scalar, vectors, idx * dimensions, dimensions);
    }

    public float get(int vectorIdx, int dimension) {
        return vectors[vectorIdx * dimensions + dimension];
    }

    public void set(int vectorIdx, int dimension, float value) {
        vectors[vectorIdx * dimensions + dimension] = value;
    }

    public void set(int vectorIdx, MemorySegment vector) {
        for (int i = 0; i < dimensions; i++) {
            vectors[vectorIdx * dimensions + i] = vector.getAtIndex(ValueLayout.JAVA_FLOAT, i);
        }
    }

    public float[] getInternalArray() {
        return vectors;
    }

    public int offset(int vectorIdx) {
        return vectorIdx * dimensions;
    }

    public float[][] toArray() {
        var array = new float[count][dimensions];
        for (int i = 0; i < count; i++) {
            System.arraycopy(vectors, i * dimensions, array[i], 0, dimensions);
        }
        return array;
    }

    public void fill(float value) {
        Arrays.fill(vectors, value);
    }

    public FloatVectorSegment copy() {
        return new FloatVectorSegment(count, dimensions, vectors.clone());
    }

    public boolean equals(int idx1, FloatVectorSegment v2, int idx2) {
        for (int i = 0; i < dimensions; i++) {
            if (Math.abs(this.get(idx1, i) - v2.get(idx2, i)) > VectorOperations.PRECISION) {
                return false;
            }
        }
        return true;
    }

    public boolean equals(FloatVectorSegment v2) {
        if (count != v2.count) return false;
        if (dimensions != v2.dimensions) return false;
        for (int i = 0; i < count; i++) {
            if (!equals(i, v2, i)) return false;
        }
        return true;
    }

    public static FloatVectorSegment makeSegment(int count, int dimensions) {
        return new FloatVectorSegment(count, dimensions, new float[count * dimensions]);
    }

    public static FloatVectorSegment[] makeSegments(int segmentCount, int vectorsPerSegment, int dimensions) {
        var result = new FloatVectorSegment[segmentCount];
        for (int i = 0; i < segmentCount; i++) {
            result[i] = new FloatVectorSegment(vectorsPerSegment, dimensions, new float[vectorsPerSegment * dimensions]);
        }
        return result;
    }
}
