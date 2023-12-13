package jetbrains.vectoriadb.index

import kotlin.math.min

class CodebookInitializer(val codebookCount: Int, vectorCount: Int, val vectorDimensions: Int) {

    val codeBaseSize: Int
    val codebookDimensions: IntArray
    val codebookDimensionOffset: IntArray
    val codebooks: Array<Array<FloatArray>>
    val maxCodebookDimensions: Int

    init {
        if (codebookCount < 0) throw IllegalArgumentException("codebookCount < 0")
        if (codebookCount > vectorDimensions) throw IllegalArgumentException("codebookCount > vectorDimensions")
        /*
         * It is possible that vectorDimensions can not be divided by codebookCount
         * */

        codeBaseSize = getCodeBaseSize(vectorCount)
        val minCodebookDimensions = vectorDimensions / codebookCount
        val numCodebooksThatHaveOneExtraDimension = vectorDimensions % codebookCount

        codebookDimensions = IntArray(codebookCount)
        codebookDimensionOffset = IntArray(codebookCount)
        for (codebookIdx in 0 until codebookCount) {
            if (codebookIdx < numCodebooksThatHaveOneExtraDimension) {
                codebookDimensions[codebookIdx] = minCodebookDimensions + 1
            } else {
                codebookDimensions[codebookIdx] = minCodebookDimensions
            }
            if (codebookIdx > 0) {
                codebookDimensionOffset[codebookIdx] = codebookDimensionOffset[codebookIdx - 1] + codebookDimensions[codebookIdx - 1]
            }
        }

        maxCodebookDimensions = codebookDimensions[0]
        codebooks = Array(codebookCount) {
            Array(codeBaseSize) { FloatArray(maxCodebookDimensions) }
        }

        assert(codebookDimensions.sum() == vectorDimensions)
    }

    companion object {

        const val CODE_BASE_SIZE: Int = 256

        @JvmStatic
        fun getCodebookCount(vectorDimensions: Int, compressionRatio: Int): Int {
            val vectorSizeBytes = vectorDimensions * java.lang.Float.BYTES
            return vectorSizeBytes / compressionRatio
        }

        @JvmStatic
        fun getCodeBaseSize(vectorCount: Int): Int = min(CODE_BASE_SIZE, vectorCount)
    }
}