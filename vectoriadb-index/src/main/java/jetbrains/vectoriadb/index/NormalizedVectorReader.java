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

import org.jetbrains.annotations.NotNull;

import java.lang.foreign.MemorySegment;

import static java.lang.Math.abs;

class NormalizedVectorReader implements VectorReader {

    private final VectorReader source;

    private float[] vectorNorms;

    public NormalizedVectorReader(VectorReader source) {
        this.source = source;
    }

    @Override
    public int size() {
        return source.size();
    }

    @Override
    public int dimensions() {
        return source.dimensions();
    }

    public void precalculateOriginalVectorNorms(@NotNull ParallelBuddy pBuddy, @NotNull ProgressTracker progressTracker) {
        vectorNorms = new float[source.size()];
        pBuddy.runSplitEvenly(
                "Original vector norms pre-calculation",
                size(),
                progressTracker,
                (_, vectorIdx) -> {
                    var vector = source.read(vectorIdx);
                    var norm = VectorOperations.calculateL2Norm(vector, dimensions());
                    vectorNorms[vectorIdx] = norm;
                }
        );
    }

    public float getOriginalVectorNorm(int index) {
        return vectorNorms[index];
    }

    @Override
    public MemorySegment read(int index) {
        var vector = source.read(index);
        var norm = vectorNorms[index];
        if (abs(norm - 1) < VectorOperations.PRECISION) {
            return vector;
        }
        var resultArr = new float[dimensions()];
        VectorOperations.normalizeL2(vector, norm, resultArr, dimensions());

        return MemorySegment.ofArray(resultArr);
    }


    @Override
    public float read(int vectorIdx, int dimension) {
        var originalValue = source.read(vectorIdx, dimension);
        var norm = vectorNorms[vectorIdx];
        return originalValue / norm;
    }

    @Override
    public MemorySegment id(int index) {
        return source.id(index);
    }

    @Override
    public void close() {}
}
