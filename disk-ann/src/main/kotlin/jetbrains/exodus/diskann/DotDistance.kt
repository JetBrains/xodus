package jetbrains.exodus.diskann

internal class DotDistance : DistanceFunction {
    override fun computeDistance(firstVector: FloatArray, secondVector: FloatArray): Double {
        var distance = 0.0

        for (i in firstVector.indices) {
            distance += firstVector[i] * secondVector[i]
        }

        return -distance
    }

    override fun computeDistance(
        firstVector: FloatArray,
        firstVectorFrom: Int,
        secondVector: FloatArray,
        secondVectorFrom: Int,
        size: Int
    ): Double {
        var distance = 0.0

        for (i in firstVectorFrom until firstVectorFrom + size) {
            distance += firstVector[i] * secondVector[i - firstVectorFrom + secondVectorFrom]
        }

        return -distance
    }
}