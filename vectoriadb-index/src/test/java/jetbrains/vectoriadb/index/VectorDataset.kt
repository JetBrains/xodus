package jetbrains.vectoriadb.index

import jetbrains.vectoriadb.index.bench.VectorDatasetInfo
import jetbrains.vectoriadb.index.bench.downloadDatasetArchives
import jetbrains.vectoriadb.index.bench.readVectors
import java.nio.file.Files
import java.nio.file.Path

abstract class VectorDataset {
    abstract fun build(): VectorDatasetContext

    data object Sift10K: VectorDataset() {
        override fun build(): VectorDatasetContext {
            val vectors = VectorDatasetInfo.Sift10K.readBaseVectorsForTest()
            val vectorReader = FloatArrayToByteArrayVectorReader(vectors)
            return VectorDatasetContext(
                vectors,
                vectorReader,
                vectors.count(),
                LoadVectorsUtil.SIFT_VECTOR_DIMENSIONS,
                maxInnerProduct = 261205.0f
            )
        }
    }

    data object Sift1M: VectorDataset() {
        override fun build(): VectorDatasetContext {
            val vectors = VectorDatasetInfo.Sift1M.readBaseVectorsForTest()
            val vectorReader = FloatArrayToByteArrayVectorReader(vectors)
            return VectorDatasetContext(
                vectors,
                vectorReader,
                vectors.count(),
                LoadVectorsUtil.SIFT_VECTOR_DIMENSIONS,
                // todo calculate
                maxInnerProduct = 261205.0f
            )
        }
    }
}

private fun VectorDatasetInfo.readBaseVectorsForTest(): Array<FloatArray> {
    val buildDirStr = System.getProperty("exodus.tests.buildDirectory")
        ?: throw RuntimeException("exodus.tests.buildDirectory is not set")

    val targetDir = Path.of(buildDirStr).resolve(name)

    Files.createDirectories(targetDir)

    downloadDatasetArchives(targetDir).forEach { archive ->
        archive.extractTo(targetDir)
    }

    val baseFilePath = targetDir.resolve(baseFile)
    return readVectors(baseFilePath, vectorDimensions, vectorCount)
}