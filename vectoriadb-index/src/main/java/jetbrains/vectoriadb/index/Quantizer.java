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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface Quantizer extends AutoCloseable {
    int CODE_BASE_SIZE = 256;

    // Initialize, make PQ code for the vectors

    void generatePQCodes(int compressionRatio, VectorReader vectorReader, @NotNull ProgressTracker progressTracker);


    // Calculate distances using the lookup table

    float[] blankLookupTable();

    void buildLookupTable(float[] vector, float[] lookupTable, DistanceFunction distanceFunction);

    float computeDistanceUsingLookupTable(float[] lookupTable, int vectorIndex);

    void computeDistance4BatchUsingLookupTable(float[] lookupTable, int vectorIndex1, int vectorIndex2, int vectorIndex3, int vectorIndex4, float[] result);


    // PQ k-means clustering

    /**
     * Every vector is included into the two closest partitions
     * */
    VectorsByPartitions splitVectorsByPartitions(VectorReader vectorReader, int numClusters, int iterations, DistanceFunction distanceFunction, @NotNull ProgressTracker progressTracker);

    float[][] calculateCentroids(int clustersCount, int iterations, DistanceFunction distanceFunction, @NotNull ProgressTracker progressTracker);


    // Other

    float[] getVectorApproximation(int vectorIdx);

    void load(DataInputStream dataInputStream) throws IOException;

    void store(DataOutputStream dataOutputStream) throws IOException;

    @Override
    void close();
}
