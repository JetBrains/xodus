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
package jetbrains.vectoriadb.index.bench

import jetbrains.vectoriadb.index.Distance
import java.nio.file.Path
import kotlin.io.path.nameWithoutExtension

sealed class VectorDatasetInfo(
    val name: String,
    val archives: List<Archive>,
    val baseFile: String,
    val learnFile: String,
    val queryFile: String,
    val l2GroundTruthFile: String,
    val ipGroundTruthFile: String,
    val vectorDimensions: Int,
    val vectorCount: Int
) {
    companion object {
        @JvmStatic
        val DEFAULT_NEIGHBOURS_COUNT: Int = 5
    }

    open val dataFile: String = Path.of(baseFile).nameWithoutExtension

    fun groundTruthFile(distance: Distance): String = when (distance) {
        Distance.L2 -> l2GroundTruthFile
        Distance.DOT -> ipGroundTruthFile
        else -> throw IllegalArgumentException("Ground truth is not supported for $distance distance function")
    }

    open fun defaultIndexName(distance: Distance): String = when (distance) {
        Distance.L2 -> "index-l2"
        Distance.DOT -> "index-ip"
        else -> throw IllegalArgumentException("Index is not supported for $distance distance")
    }

    abstract val datasetSource: VectorDatasetSource

    object Sift10K: SmallIrisaFrDataset("siftsmall", 128, 10_000)

    object Sift1M: SmallIrisaFrDataset("sift", 128, 1_000_000)

    object Gist1M: SmallIrisaFrDataset("gist", 960, 1_000_000)

    abstract class SmallIrisaFrDataset(name: String, vectorDimensions: Int, vectorCount: Int): VectorDatasetInfo(
        name = name,
        archives = listOf(
            Archive(archiveName = "${name}.tar.gz", fileInside = "${name}_base.fvecs")
        ),
        baseFile = "${name}_base.fvecs",
        learnFile = "${name}_learn.fvecs",
        queryFile = "${name}_query.fvecs",
        l2GroundTruthFile = "${name}_groundtruth.ivecs",
        ipGroundTruthFile = "${name}_ip_groundtruth.ivecs",
        vectorDimensions = vectorDimensions,
        vectorCount = vectorCount
    ) {
        override val datasetSource: VectorDatasetSource = IrisaFrDatasetSource
    }

    class BigANN(
        private val vectorCountMillions: Int
    ): VectorDatasetInfo(
        name = "bigann${vectorCountMillions}m",
        archives = listOf(
            // 92GB before extracting, do not download it on your mum PC
            Archive("bigann_base.bvecs.gz", "bigann_base.bvecs"),
            // we do not use the learn dataset, so lets skip it, as it takes 9GB after all
            // Archive("bigann_learn.bvecs.gz", "bigann_learn.bvecs"),
            Archive("bigann_query.bvecs.gz", "bigann_query.bvecs"),
            Archive("bigann_gnd.tar.gz", "idx_1M.ivecs"),
        ),
        baseFile = "bigann_base.bvecs",
        learnFile = "bigann_learn.bvecs",
        queryFile = "bigann_query.bvecs",
        l2GroundTruthFile = "idx_${vectorCountMillions}M.ivecs",
        ipGroundTruthFile = "idx_ip_${vectorCountMillions}M.ivecs",
        vectorDimensions = 128,
        vectorCount = vectorCountMillions * 1_000_000
    ) {
        override val dataFile: String = "${Path.of(baseFile).nameWithoutExtension}_${vectorCountMillions}M"

        override val datasetSource: VectorDatasetSource = IrisaFrDatasetSource

        override fun defaultIndexName(distance: Distance): String = when (distance) {
            Distance.L2 -> "index-${vectorCountMillions}m-l2"
            Distance.DOT -> "index-${vectorCountMillions}m-ip"
            else -> throw IllegalArgumentException("Index is not supported for $distance distance")
        }
    }
}

fun String.toDatasetContext(): VectorDatasetInfo {
    val datasetName = this.lowercase().trim()
    return when {
        datasetName == "sift10k" -> VectorDatasetInfo.Sift10K
        datasetName == "sift1m" -> VectorDatasetInfo.Sift1M
        datasetName == "gist1m" -> VectorDatasetInfo.Gist1M
        datasetName.startsWith("bigann") -> {
            val vectorCountMillions = datasetName.removePrefix("bigann").removeSuffix("m").toIntOrNull()
            if (vectorCountMillions == null || vectorCountMillions < 1 || vectorCountMillions > 1000) {
                throw IllegalArgumentException("$this dataset is not supported")
            }
            VectorDatasetInfo.BigANN(vectorCountMillions)
        }
        else -> throw IllegalArgumentException("$this dataset is not supported")
    }
}