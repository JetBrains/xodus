package jetbrains.exodus.diskann

internal class L2Distance : DistanceFunction {
    override fun computeDistance(firstVector: FloatArray, secondVector: FloatArray): Double {
        var distance = 0.0

        for (i in firstVector.indices) {
            val diff = firstVector[i] - secondVector[i]
            distance += diff * diff
        }

        return Math.sqrt(distance)
    }

    override fun computeDistance(
        firstVector: FloatArray,
        firstVectorFrom: Int,
        secondVector: FloatArray,
        secondVectorFrom: Int
    ): Double {
        var distance = 0.0

        for (i in firstVectorFrom until firstVector.size) {
            val diff = firstVector[i] - secondVector[i - firstVectorFrom + secondVectorFrom]
            distance += diff * diff
        }

        return Math.sqrt(distance)
    }
}