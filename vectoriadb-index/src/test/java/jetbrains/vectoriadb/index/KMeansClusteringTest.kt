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
package jetbrains.vectoriadb.index

import org.junit.Test

class KMeansClusteringTest {

    @Test
    fun kMeansClustering() = parallelVectorTest(VectorDataset.Sift10K) {
        val distance = L2DistanceFunctionNew()

        val maxIteration = 50
        val numClusters = 33
        val centroids = FloatVectorSegment.makeSegment(numClusters, dimensions)
        val centroidIdxByVectorIdx = ByteCodeSegment.makeArraySegment(numVectors)

        val kmeans = KMeansClustering(
            "test clustering",
            distance,
            vectorReader,
            centroids,
            centroidIdxByVectorIdx,
            maxIteration,
            pBuddy
        )

        kmeans.calculateCentroids(progressTracker)

        val randomCentroids = makeRandomCentroids(vectors, numClusters)

        val coef = silhouetteCoefficient(distance, centroids.toArray(), vectors)
        val randomCoef = silhouetteCoefficient(distance, randomCentroids, vectors)

        println("coef: $coef, randomCoef: $randomCoef")
        assert(coef > randomCoef)
    }
}