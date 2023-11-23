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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.jetbrains.annotations.NotNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * Ignore the compressionRation problem. Use additional space, you will fix (maybe) this problem later when everything works.
 *
 * */
public final class DotDistancePQQuantizer extends AbstractQuantizer {

    // Initialize, make PQ code for the vectors

    @Override
    protected MemorySegment allocateMemoryForPqVectors(int quantizersCount, int vectorsCount, Arena arena) {
        // todo It is used only internally in generatePQCodes(), so maybe hide it from the public interface.

        // The implementation is just to allocate memory only for the norm quantization.
        // We should not call L2 instance because it will call this method itself
        return null;
    }

    @Override
    public void generatePQCodes(int vectorsDimension, int compressionRatio, VectorReader vectorReader, @NotNull ProgressTracker progressTracker) {
        // 1. Create two private L2 quantizers: normalizedL2 and originalL2
        // 2. Create a wrapper normalizedVectorReader, that wraps vectorReader and returns normalized vectors,
        // it can remember vector norms on the way, so you will not have to recalculate them once more.
        // 3. normalizedL2.generatePQCodes(normalizedVectorReader), originalL2.generatePQCodes(vectorReader)
        // 4. Calculate relative norm lx=||X||/||approximatedX'||. Go through normalizedL2.codes, decode vectors, calculate norm for them
        // get original norm from normalizedVectorReader.
        // 5. Train norm codebooks to quantize lx. Provide the possibility to have an arbitrary number of codebooks (1 and > 1).
        // Train codebooks using k-means. lx - is a vector with 1 dimension.
    }


    // Calculate distances using the lookup table

    @Override
    public float[] blankLookupTable() {
        // It is used when initializing a thread local NearestGreedySearchCachedData in the IndexReader constructor.
        // It is filled in IndexReader.nearest() when doing the search

        // Delegate to the L2 implementation and return the result.
        return new float[0];
    }

    @Override
    public void buildLookupTable(float[] vector, float[] lookupTable, DistanceFunction distanceFunction) {
        // It is used in IndexReader.nearest() when doing the search.

        // Delegate to the L2 implementation
        // So we can calculate the inner product for `q` and the approximation of X'(normalized X)
    }

    @Override
    public float computeDistanceUsingLookupTable(float[] lookupTable, int vectorIndex) {
        // It is used in IndexReader.nearest() when doing the search.

        // 1. Delegate to the L2 implementation, so you have `p` - the inner product for `q` and the approximation of the normalized vector `vectorIndex`
        // 2. Calculate `l` - approximation of the norm for `vectorIndex` using the norm codebooks
        // 3. return l*p
        return 0;
    }

    @Override
    public void computeDistance4BatchUsingLookupTable(float[] lookupTable, int vectorIndex1, int vectorIndex2, int vectorIndex3, int vectorIndex4, float[] result) {
        // It is used in IndexReader.nearest() when doing the search.

        // do the same as computeDistanceUsingLookupTable() but for the four vectors
    }


    // PQ k-means clustering

    @Override
    public IntArrayList[] splitVectorsByPartitions(int numClusters, int iterations, DistanceFunction distanceFunction, @NotNull ProgressTracker progressTracker) {
        // It is used in IndexBuilder.buildIndex() to split the vectors into partitions to build graph indices for every partition separately.
        // The whole graph index does not fit memory, so we have to split vectors into partitions.

        // Use an extra L2 quantazier that is trained on the original vectors.
        // Delegate the job to it.

        return new IntArrayList[0];
    }

    @Override
    public float[][] calculateCentroids(int clustersCount, int iterations, DistanceFunction distanceFunction, @NotNull ProgressTracker progressTracker) {
        // It is used in IndexBuilder.buildIndex() to calculate the graph medoid to start the search from.
        // And also in tests to test the quality of k-means clustering

        // Use an extra L2 quantazier that is trained on the original vectors.
        // Delegate the job to it.

        return new float[0][];
    }

    @Override
    public float[] decodeVector(byte[] vectors, int vectorIndex) {
        // todo It is used only internally in calculateCentroids() to calculate the approximation of the result pqCentroids, so maybe hide it from the public interface.

        // We cannot implement this method because the param vectors does not contain norm codes.
        // But it is not a big deal as it is not an actually public api.
        // So we can delegate this method to L2 quantazier that is trained on the original vectors.
        return new float[0];
    }


    // Store/load/close

    @Override
    public void load(DataInputStream dataInputStream) throws IOException {
        // 1. Delegate to the L2
        // 2. Load the local state of this instance
    }

    @Override
    public void store(DataOutputStream dataOutputStream) throws IOException {
        // 1. Delegate to the L2
        // 2. Store the local state of this instance
    }

    @Override
    public void close() {
        // do the same as L2 quantizer
    }
}
