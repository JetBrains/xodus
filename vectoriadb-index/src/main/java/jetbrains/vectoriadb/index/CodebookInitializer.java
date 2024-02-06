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

import java.util.Arrays;

public class CodebookInitializer {
    public static final int CODE_BASE_SIZE = 256;

    private final int codebookCount;
    private final int vectorDimensions;
    private final int codeBaseSize;
    private final int[] codebookDimensions;
    private final int[] codebookDimensionOffset;
    private final float[][][] codebooks;
    private final int maxCodebookDimensions;

    @SuppressWarnings("unused")
    public int getCodebookCount() {
        return codebookCount;
    }

    public int getCodeBaseSize() {
        return codeBaseSize;
    }

    public int getVectorDimensions() {
        return vectorDimensions;
    }

    public int[] getCodebookDimensions() {
        return codebookDimensions;
    }

    public int[] getCodebookDimensionOffset() {
        return codebookDimensionOffset;
    }

    public float[][][] getCodebooks() {
        return codebooks;
    }

    public int getMaxCodebookDimensions() {
        return maxCodebookDimensions;
    }

    public CodebookInitializer(int codebookCount, int vectorCount, int vectorDimensions) {
        if (codebookCount < 0) {
            throw new IllegalArgumentException("codebookCount < 0");
        }
        if (codebookCount > vectorDimensions) {
            throw new IllegalArgumentException("codebookCount > vectorDimensions");
        }

        this.codebookCount = codebookCount;
        this.vectorDimensions = vectorDimensions;
        this.codeBaseSize = getCodeBaseSize(vectorCount);

        int minCodebookDimensions = vectorDimensions / codebookCount;
        int numCodebooksThatHaveOneExtraDimension = vectorDimensions % codebookCount;

        this.codebookDimensions = new int[codebookCount];
        this.codebookDimensionOffset = new int[codebookCount];

        for (int codebookIdx = 0; codebookIdx < codebookCount; codebookIdx++) {
            if (codebookIdx < numCodebooksThatHaveOneExtraDimension) {
                this.codebookDimensions[codebookIdx] = minCodebookDimensions + 1;
            } else {
                this.codebookDimensions[codebookIdx] = minCodebookDimensions;
            }
            if (codebookIdx > 0) {
                this.codebookDimensionOffset[codebookIdx] = codebookDimensionOffset[codebookIdx - 1] + codebookDimensions[codebookIdx - 1];
            }
        }
        this.maxCodebookDimensions = codebookDimensions[0];
        this.codebooks = new float[codebookCount][codeBaseSize][maxCodebookDimensions];
        assert Arrays.stream(codebookDimensions).sum() == vectorDimensions;
    }

    public static int getCodebookCount(int vectorDimensions, int compressionRatio) {
        int vectorSizeBytes = vectorDimensions * Float.BYTES;
        return vectorSizeBytes / compressionRatio;
    }

    public static int getCodeBaseSize(int vectorCount) {
        return Math.min(CODE_BASE_SIZE, vectorCount);
    }
}
