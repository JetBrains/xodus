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

import org.apache.commons.rng.simple.RandomSource;
import org.jetbrains.annotations.NotNull;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

class RandomClusterInitializer implements ClusterInitializer {
    @Override
    public void initializeCentroids(MemorySegment pqVectors, long numVectors, float[] distanceTable, int quantizersCount, int codeBaseSize, byte[] pqCentroids, int numClusters, @NotNull ProgressTracker progressTracker) {
        var rng = RandomSource.XO_RO_SHI_RO_128_PP.create();
        for (int i = 0; i < numClusters; i++) {
            var vecIndex = rng.nextLong(numVectors);
            MemorySegment.copy(pqVectors, ValueLayout.JAVA_BYTE, vecIndex * quantizersCount, pqCentroids, i * quantizersCount, quantizersCount);
        }
    }
}