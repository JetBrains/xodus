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
import jetbrains.vectoriadb.index.siftbench.SiftBenchUtils;
import org.junit.Assert;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class AbstractVectorsTest {
    public static int SIFT_VECTOR_DIMENSIONS = 128;

    public static float[][] loadSift10KVectors() throws IOException {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!");
        }

        var siftArchive = "siftsmall.tar.gz";
        SiftBenchUtils.downloadSiftBenchmark(siftArchive, buildDir);

        var siftSmallDir = SiftBenchUtils.extractSiftDataSet(siftArchive, buildDir);

        var siftDir = "siftsmall";
        var sifSmallFilesDir = siftSmallDir.toPath().resolve(siftDir);

        var siftBaseName = "siftsmall_base.fvecs";
        var siftSmallBase = sifSmallFilesDir.resolve(siftBaseName);

        System.out.println("Reading data vectors...");

        var vectors = SiftBenchUtils.readFVectors(siftSmallBase, SIFT_VECTOR_DIMENSIONS);

        System.out.printf("%d data vectors loaded with dimension %d%n",
                vectors.length, SIFT_VECTOR_DIMENSIONS);

        return vectors;
    }

    @SuppressWarnings("unused")
    public static float[][] loadSift1MVectors() throws IOException {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!");
        }

        var siftArchive = "sift.tar.gz";
        SiftBenchUtils.downloadSiftBenchmark(siftArchive, buildDir);

        var siftSmallDir = SiftBenchUtils.extractSiftDataSet(siftArchive, buildDir);

        var siftDir = "sift";
        var sifSmallFilesDir = siftSmallDir.toPath().resolve(siftDir);

        var siftBaseName = "sift_base.fvecs";
        var siftSmallBase = sifSmallFilesDir.resolve(siftBaseName);

        System.out.println("Reading data vectors...");

        var vectors = SiftBenchUtils.readFVectors(siftSmallBase, SIFT_VECTOR_DIMENSIONS);

        System.out.printf("%d data vectors loaded with dimension %d%n",
                vectors.length, SIFT_VECTOR_DIMENSIONS);

        return vectors;
    }

    public static float avgDistance(IntArrayList fromVectors, IntArrayList toVectors, float[][] vectors,
                                    DistanceFunction distanceFunction) {
        var sum = 0.0f;
        var count = 0;

        for (var vi : fromVectors) {
            for (var wi : toVectors) {
                var v = vectors[vi];
                var w = vectors[wi];
                if (!Arrays.equals(v, w)) {
                    sum += distanceFunction.computeDistance(v, 0, w, 0, v.length);
                    count++;
                }
            }
        }

        return sum / count;
    }

    public static float intraClusterDistance(IntArrayList vectorsInCluster, float[][] vectors, DistanceFunction distanceFunction) {
        return avgDistance(vectorsInCluster, vectorsInCluster, vectors, distanceFunction);
    }

    public static float interClusterDistance(int clusterIndex, List<IntArrayList> vectorsInClusters,
                                             int[] closestClusters,
                                             float[][] vectors,
                                             DistanceFunction distanceFunction) {
        var sum = 0.0f;
        var count = 0;

        var vectorsInCluster = vectorsInClusters.get(clusterIndex);
        for (int i = 0; i < vectorsInCluster.size(); i++) {
            var vi = vectorsInCluster.getInt(i);
            var closestClusterIndex = closestClusters[vi];

            assert closestClusterIndex != clusterIndex;

            sum += avgDistance(vectorsInCluster, vectorsInClusters.get(closestClusterIndex), vectors, distanceFunction);
            count++;
        }

        return sum / count;
    }

    public static float silhouetteCoefficient(List<IntArrayList> clusters,
                                              int[] closestClusters, float[][] vectors,
                                              DistanceFunction distanceFunction) {
        var sum = 0.0f;

        for (int i = 0; i < clusters.size(); i++) {
            sum += clusterSilhouetteCoefficient(i, clusters, closestClusters, vectors, distanceFunction);
        }

        return sum / clusters.size();
    }

    public static float clusterSilhouetteCoefficient(int clusterIndex,
                                                     List<IntArrayList> clusters,
                                                     int[] closestClusters,
                                                     float[][] vectors,
                                                     DistanceFunction distanceFunction) {
        var a = intraClusterDistance(clusters.get(clusterIndex), vectors, distanceFunction);
        var b = interClusterDistance(clusterIndex, clusters, closestClusters, vectors, distanceFunction);

        return (b - a) / Math.max(a, b);
    }

    public static int[] findClosestAndSecondClosestCluster(float[][] centroids, float[] vector,
                                                           DistanceFunction distanceFunction) {
        var closestClusterIndex = -1;
        var secondClosestClusterIndex = -1;
        var closestDistance = Float.MAX_VALUE;
        var secondClosestDistance = Float.MAX_VALUE;

        for (var i = 0; i < centroids.length; i++) {
            var centroid = centroids[i];
            var distance = distanceFunction.computeDistance(centroid, 0, vector,
                    0, centroid.length);

            if (distance < closestDistance) {
                secondClosestClusterIndex = closestClusterIndex;
                secondClosestDistance = closestDistance;
                closestClusterIndex = i;
                closestDistance = distance;
            } else if (distance < secondClosestDistance) {
                secondClosestClusterIndex = i;
                secondClosestDistance = distance;
            }
        }

        assert closestClusterIndex != secondClosestClusterIndex;
        return new int[]{closestClusterIndex, secondClosestClusterIndex};
    }
}
