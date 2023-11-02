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

import jetbrains.vectoriadb.index.siftbench.SiftBenchUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.io.IOException;

public class AbstractVectorsTest {
    public static int SIFT_VECTOR_DIMENSIONS = 128;
    public static int GIST_VECTOR_DIMENSIONS = 960;

    public static float[][] loadSift10KVectors() throws IOException {
        return loadVectors("siftsmall", SIFT_VECTOR_DIMENSIONS);
    }

    @SuppressWarnings("unused")
    public static float[][] loadSift1MVectors() throws IOException {
        return loadVectors("sift", SIFT_VECTOR_DIMENSIONS);
    }

    public static float[][] loadGist1MVectors() throws IOException {
        return loadVectors("gist", GIST_VECTOR_DIMENSIONS);
    }

    private static float[][] loadVectors(@NotNull String name, int vectorDimensions) throws IOException {
        var buildDir = System.getProperty("exodus.tests.buildDirectory");
        if (buildDir == null) {
            Assert.fail("exodus.tests.buildDirectory is not set !!!");
        }

        var archive = name + ".tar.gz";
        SiftBenchUtils.downloadSiftBenchmark(archive, buildDir);

        var dir = SiftBenchUtils.extractSiftDataSet(archive, buildDir);

        var filesDir = dir.toPath().resolve(name);

        var baseName = name + "_base.fvecs";
        var basePath = filesDir.resolve(baseName);

        System.out.println("Reading data vectors...");

        var vectors = SiftBenchUtils.readFVectors(basePath, vectorDimensions);

        System.out.printf("%d data vectors loaded with dimension %d%n", vectors.length, vectorDimensions);

        return vectors;
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
