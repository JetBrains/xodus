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

class ScalarArrayReader implements VectorReader {

    @NotNull
    private final float[] values;

    public ScalarArrayReader(@NotNull float[] values) {
        this.values = values;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public int dimensions() {
        return 1;
    }

    @Override
    public float read(int vectorIdx, int dimension) {
        return values[vectorIdx];
    }

    @Override
    public MemorySegment read(int index) {
        return MemorySegment.ofArray(new float[] { values[index] });
    }

    @Override
    public MemorySegment id(int index) {
        // this vector reader should not be used in a context where vector id is required
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {

    }
}
