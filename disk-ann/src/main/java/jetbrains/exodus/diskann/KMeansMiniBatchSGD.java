package jetbrains.exodus.diskann;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.rng.simple.RandomSource;

import java.util.Arrays;
import java.util.BitSet;

final class KMeansMiniBatchSGD {
    final float[][] centroids;
    final int k;
    final VectorReader vectorReader;
    final int subVecOffset;

    final int[] centroidsSamplesCount;

    private int currentIndex;
    private int iteration;

    KMeansMiniBatchSGD(int k, int subVecOffset, int subVecSize, VectorReader vectorReader) {
        this.k = k;
        this.vectorReader = vectorReader;
        this.subVecOffset = subVecOffset;
        this.centroidsSamplesCount = new int[k];


        var rng = RandomSource.XO_RO_SHI_RO_128_PP.create(42);

        var size = vectorReader.size();
        if (size <= k) {
            centroids = new float[size][subVecSize];
            for (int i = 0; i < size; i++) {
                var vector = vectorReader.read(i);
                centroids[i] = Arrays.copyOfRange(vector, subVecOffset, subVecOffset + subVecSize);
            }
        } else if (size < 4 * k) {
            centroids = new float[k][subVecSize];
            var indexes = new int[size];

            for (int i = 0; i < size; i++) {
                indexes[i] = i;
            }

            ArrayUtils.shuffle(indexes);

            for (int i = 0; i < k; i++) {
                var centroidIndex = indexes[i];
                var vector = vectorReader.read(centroidIndex);
                centroids[i] = Arrays.copyOfRange(vector, subVecOffset, subVecOffset + subVecSize);
            }
        } else {
            centroids = new float[k][subVecSize];
            var bitSet = new BitSet(size);
            for (int i = 0; i < k; i++) {
                int centroidIndex;
                do {
                    centroidIndex = rng.nextInt(size);
                } while (bitSet.get(centroidIndex));
                bitSet.set(centroidIndex);

                var vector = vectorReader.read(centroidIndex);
                centroids[i] = Arrays.copyOfRange(vector, subVecOffset, subVecOffset + subVecSize);
            }
        }


    }

    int nextIteration(@SuppressWarnings("SameParameterValue") int batchSize, byte distanceFunction) {
        var actualBatchSize = Math.min(currentIndex + batchSize, vectorReader.size()) - currentIndex;

        var vectors = new float[actualBatchSize][];
        var centroidIds = new int[actualBatchSize];

        for (int i = 0; i < actualBatchSize; i++) {
            var vector = vectorReader.read(currentIndex + i);
            vectors[i] = vector;
            centroidIds[i] = DiskANN.findClosestCentroid(distanceFunction, centroids, vector, subVecOffset);
        }


        for (int i = 0; i < actualBatchSize; i++) {
            var centroid = centroidIds[i];
            centroidsSamplesCount[centroid]++;

            var learningRate = 1.0f / centroidsSamplesCount[centroid];

            var after = DiskANN.computeGradientStep(centroids[centroid], vectors[i], subVecOffset, learningRate);
            centroids[centroid] = after;
        }

        currentIndex += actualBatchSize;
        if (currentIndex == vectorReader.size()) {
            iteration++;
        }

        return iteration;
    }


}
