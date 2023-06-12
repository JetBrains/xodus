package jetbrains.exodus.diskann

import jdk.incubator.vector.FloatVector
import jdk.incubator.vector.VectorOperators
import jdk.incubator.vector.VectorSpecies

internal class L2Distance : DistanceFunction {
    override fun computeDistance(firstVector: FloatArray, secondVector: FloatArray): Double {
        var sumVector = FloatVector.zero(species)
        var index = 0

        while (index < species.loopBound(firstVector.size)) {
            val first = FloatVector.fromArray(species, firstVector, index)
            val second = FloatVector.fromArray(species, secondVector, index)
            val diff = first.sub(second)
            val mul = diff.mul(diff)

            sumVector = sumVector.add(mul)
            index += species.length()
        }

        var sum = sumVector.reduceLanes(VectorOperators.ADD).toDouble()

        while (index < firstVector.size) {
            val diff = firstVector[index] - secondVector[index]
            sum += diff * diff
            index++
        }

        return sum
    }

    override fun computeDistance(
        firstVector: FloatArray,
        firstVectorFrom: Int,
        secondVector: FloatArray,
        secondVectorFrom: Int,
        size: Int
    ): Double {
        var sumVector = FloatVector.zero(species)
        var index = 0

        while (index < species.loopBound(size)) {
            val first = FloatVector.fromArray(species, firstVector, index + firstVectorFrom)
            val second = FloatVector.fromArray(species, secondVector, index + secondVectorFrom)
            val diff = first.sub(second)
            val mul = diff.mul(diff)

            sumVector = sumVector.add(mul)
            index += species.length()
        }

        var sum = sumVector.reduceLanes(VectorOperators.ADD).toDouble()

        while (index < size) {
            val diff = firstVector[index + firstVectorFrom] - secondVector[index + secondVectorFrom]
            sum += diff * diff
            index++
        }

        return sum
    }

    companion object {
        private val species: VectorSpecies<Float> = FloatVector.SPECIES_PREFERRED
    }
}