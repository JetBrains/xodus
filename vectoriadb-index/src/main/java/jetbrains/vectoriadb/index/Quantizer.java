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
package jetbrains.vectoriadb.index;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface Quantizer extends AutoCloseable {
    int CODE_BASE_SIZE = 256;

    float[] blankLookupTable();

    float[] decodeVector(byte[] vectors, int vectorIndex);

    IntArrayList[] splitVectorsByPartitions(int numClusters, int iterations, DistanceFunction distanceFunction, ProgressTracker progressTracker);

    float[][] calculateCentroids(int clustersCount, int iterations, DistanceFunction distanceFunction, ProgressTracker progressTracker);

    void generatePQCodes(int vectorsDimension, int compressionRatio, VectorReader vectorReader,
                         ProgressTracker progressTracker);

    float computeDistanceUsingLookupTable(float[] lookupTable, int vectorIndex);

    void computeDistance4BatchUsingLookupTable(float[] lookupTable, int vectorIndex1, int vectorIndex2,
                                               int vectorIndex3, int vectorIndex4, float[] result);

    void buildLookupTable(float[] vector, float[] lookupTable, DistanceFunction distanceFunction);

    void load(DataInputStream dataInputStream) throws IOException;

    void store(DataOutputStream dataOutputStream) throws IOException;

    @Override
    void close();
}
