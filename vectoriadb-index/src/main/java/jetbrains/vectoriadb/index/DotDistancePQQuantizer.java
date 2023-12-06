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
 */
class DotDistancePQQuantizer extends AbstractQuantizer {

    private final Arena arena;

    private final int numWorkers;
    private final int kMeansMaxIteration = 50;

    // used for the inner product calculation, needed at the search time
    private final L2PQQuantizer normalizedL2 = new L2PQQuantizer();

    private final int normCodebooksCount;
    private FloatVectorSegment[] codebooks;
    private CodeSegment[] codes;

    public DotDistancePQQuantizer() {
        this(1);
    }

    DotDistancePQQuantizer(int normCodebooksCount) {
        if (normCodebooksCount < 1) throw new IllegalArgumentException("normCodebooksCount should be greater or equal 1");
        arena = Arena.ofShared();
        this.normCodebooksCount = normCodebooksCount;
        numWorkers = ParallelExecution.availableCores();
    }

    @Override
    protected MemorySegment allocateMemoryForPqVectors(int quantizersCount, int vectorsCount, Arena arena) {
        // todo It is used only internally in generatePQCodes(), so maybe hide it from the public interface.

        // The implementation is just to allocate memory only for the norm quantization.
        // We should not call L2 instance because it will call this method itself
        return null;
    }

    @Override
    public void generatePQCodes(int vectorsDimension, int compressionRatio, VectorReader vectorReader, @NotNull ProgressTracker progressTracker) {
        progressTracker.pushPhase("Norm-explicit PQ codes creation", "norm quantizers count", String.valueOf(normCodebooksCount));

        try (
                var localArena = Arena.ofShared();
                var normalizedVectorReader = new NormalizedVectorReader(vectorReader);
                var pBuddy = new ParallelBuddy(numWorkers, "norm-explicit-pq-calc-")
        ) {
            codebooks = FloatVectorSegment.makeNativeSegments(arena, normCodebooksCount, vectorReader.size(), vectorsDimension);
            codes = ByteCodeSegment.makeNativeSegments(arena, normCodebooksCount, vectorReader.size());

            // precalculate original vector norms not to calculate them on every read
            normalizedVectorReader.precalculateOriginalVectorNorms(pBuddy, progressTracker);

            normalizedL2.generatePQCodes(vectorsDimension, compressionRatio, normalizedVectorReader, progressTracker);

            // Calculate relativeNorm = originalVectorNorm / normalizedApproximatedVectorNorm, so later we can get originalVectorNorm = relativeNorm * normalizedApproximatedVectorNorm
            var relativeNorms = FloatVectorSegment.makeNativeSegment(localArena, vectorReader.size(), 1);
            calculateRelativeNorms(pBuddy, vectorReader, normalizedVectorReader, relativeNorms, progressTracker);

            trainCodebook(pBuddy, relativeNorms, 0, progressTracker);

            if (normCodebooksCount > 1) {
                var normResiduals = FloatVectorSegment.makeNativeSegment(localArena, vectorReader.size(), 1);
                for (int codebookIdx = 1; codebookIdx < normCodebooksCount; codebookIdx++) {
                    calculateNormResiduals(pBuddy, relativeNorms, normResiduals, codebookIdx, progressTracker);
                    trainCodebook(pBuddy, normResiduals, codebookIdx, progressTracker);
                }
            }

        } finally {
            progressTracker.pullPhase();
        }
    }

    private void trainCodebook(ParallelBuddy pBuddy, FloatVectorSegment norms, int codebookIdx, ProgressTracker progressTracker) {
        var kmeans = new KMeansClustering(
                L2DistanceFunction.INSTANCE,
                new FloatVectorSegmentReader(norms),
                codebooks[codebookIdx],
                codes[codebookIdx],
                kMeansMaxIteration,
                pBuddy
        );
        kmeans.calculateCentroids(progressTracker);
    }

    private void calculateRelativeNorms(ParallelBuddy pBuddy, VectorReader vectorReader, NormalizedVectorReader normalizedVectorReader, FloatVectorSegment relativeNorms, ProgressTracker progressTracker) {
        pBuddy.run(
                "Calculate relative norms",
                vectorReader.size(),
                progressTracker,
                (_, vectorIdx) -> {
                    var normalizedVectorApproximation = normalizedL2.getVectorApproximation(vectorIdx);
                    var normalizedVectorApproximationNorm = VectorOperations.calculateL2Norm(normalizedVectorApproximation);
                    var originalVectorNorm = normalizedVectorReader.getOriginalVectorNorm(vectorIdx);
                    relativeNorms.set(vectorIdx, 0, originalVectorNorm / normalizedVectorApproximationNorm);
                }
        );
    }

    private void calculateNormResiduals(ParallelBuddy pBuddy, FloatVectorSegment relativeNorms, FloatVectorSegment normResiduals, int codebookIdx, ProgressTracker progressTracker) {
        pBuddy.run(
                "Calculate norm residuals " + codebookIdx,
                relativeNorms.count(),
                progressTracker,
                (_, vectorIdx) -> {
                    var relativeNorm = relativeNorms.get(vectorIdx, 0);
                    var relativeNormApproximation = 0f;
                    for (int i = 0; i < codebookIdx; i++) {
                        relativeNormApproximation += codebooks[i].get(vectorIdx, 0);
                    }
                    normResiduals.set(vectorIdx, 0, relativeNorm - relativeNormApproximation);
                }
        );
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
