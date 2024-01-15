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