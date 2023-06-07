package jetbrains.exodus.diskann

internal interface DistanceFunction {
    fun computeDistance(firstVector: FloatArray, secondVector: FloatArray): Double

    fun computeDistance(firstVector: FloatArray, firstVectorFrom: Int, secondVector: FloatArray, secondVectorFrom: Int) : Double
}