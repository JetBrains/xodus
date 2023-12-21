package jetbrains.vectoriadb.index.bench

import jetbrains.vectoriadb.index.Distance
import java.nio.file.Path
import kotlin.io.path.nameWithoutExtension

sealed class VectorDatasetContext(
    val archives: List<Archive>,
    val baseFile: String,
    val learnFile: String,
    val queryFile: String,
    val l2GroundTruthFile: String,
    val ipGroundTruthFile: String,
    val vectorDimensions: Int,
    val vectorCount: Int
) {

    val dataFile: String = Path.of(baseFile).nameWithoutExtension

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

    object Sift10K: SmallIrisaFrDatasetContext("siftsmall", 128, 10_000)

    object Sift1M: SmallIrisaFrDatasetContext("sift", 128, 1_000_000)

    object Gist1M: SmallIrisaFrDatasetContext("gist", 960, 1_000_000)

    abstract class SmallIrisaFrDatasetContext(name: String, vectorDimensions: Int, vectorCount: Int): VectorDatasetContext(
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
    ): VectorDatasetContext(
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
        override val datasetSource: VectorDatasetSource = IrisaFrDatasetSource

        override fun defaultIndexName(distance: Distance): String = when (distance) {
            Distance.L2 -> "index-${vectorCountMillions}m-l2"
            Distance.DOT -> "index-${vectorCountMillions}m-ip"
            else -> throw IllegalArgumentException("Index is not supported for $distance distance")
        }
    }
}

data class Archive(
    val archiveName: String,
    // fileInside lets us easily check whether the archive was extracted or not
    val fileInside: String
)

fun String.toDatasetContext(): VectorDatasetContext {
    val datasetName = this.lowercase().trim()
    return when {
        datasetName == "sift10k" -> VectorDatasetContext.Sift10K
        datasetName == "sift1m" -> VectorDatasetContext.Sift1M
        datasetName == "gist1m" -> VectorDatasetContext.Gist1M
        datasetName.startsWith("bigann") -> {
            val vectorCountMillions = datasetName.removePrefix("bigann").removeSuffix("m").toIntOrNull()
            if (vectorCountMillions == null || vectorCountMillions < 1 || vectorCountMillions > 1000) {
                throw IllegalArgumentException("$this dataset is not supported")
            }
            VectorDatasetContext.BigANN(vectorCountMillions)
        }
        else -> throw IllegalArgumentException("$this dataset is not supported")
    }
}

fun String.toDistance(): Distance = when (this.lowercase().trim()) {
    "l2" -> Distance.L2
    "ip" -> Distance.DOT
    else -> throw IllegalArgumentException("$this distance is not supported")
}