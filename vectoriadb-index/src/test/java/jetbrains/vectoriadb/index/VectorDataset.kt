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

import jetbrains.vectoriadb.index.bench.VectorDatasetInfo

abstract class VectorDataset {
    abstract fun build(): VectorDatasetContext

    data object Sift10K: VectorDataset() {
        override fun build(): VectorDatasetContext {
            val dataset = VectorDatasetInfo.Sift10K
            val vectors = dataset.readBaseVectors()
            val vectorReader = FloatArrayVectorReader(vectors)
            return VectorDatasetContext(
                vectors,
                vectorReader,
                vectors.count(),
                dataset.vectorDimensions,
                maxInnerProduct = 261205.0f
            )
        }
    }

    data object Sift1M: VectorDataset() {
        override fun build(): VectorDatasetContext {
            val dataset = VectorDatasetInfo.Sift1M
            val vectors = dataset.readBaseVectors()
            val vectorReader = FloatArrayVectorReader(vectors)
            return VectorDatasetContext(
                vectors,
                vectorReader,
                vectors.count(),
                dataset.vectorDimensions,
                // todo calculate
                maxInnerProduct = 261205.0f
            )
        }
    }
}