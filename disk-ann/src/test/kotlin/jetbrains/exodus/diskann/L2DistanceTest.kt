package jetbrains.exodus.diskann

import jdk.incubator.vector.FloatVector
import jdk.incubator.vector.VectorSpecies
import org.junit.Assert
import org.junit.Test

class L2DistanceTest {
    @Test
    fun testComputeDistanceNoLanes() {
        val l2Distance = L2Distance()
        val firstVector = floatArrayOf(1.0f, 2.0f, 3.0f)
        val secondVector = floatArrayOf(4.0f, 5.0f, 6.0f)
        val distance = l2Distance.computeDistance(firstVector, secondVector)
        Assert.assertEquals(27.0, distance, 0.0)
    }

    @Test
    fun testComputeDistanceWithNonZeroOffsetsNoLanes() {
        val l2Distance = L2Distance()
        val firstVector = floatArrayOf(1.0f, 2.0f, 3.0f)
        val secondVector = floatArrayOf(4.0f, 5.0f, 6.0f)
        val distance = l2Distance.computeDistance(firstVector, 1, secondVector, 1, 1)
        Assert.assertEquals(9.0, distance, 0.0)
    }

    @Test
    fun testComputeDistanceWitLanes() {
        val l2Distance = L2Distance()
        val laneSize = species.length()
        val firstVector = FloatArray(laneSize + 2)

        for (i in firstVector.indices) {
            firstVector[i] = i * 1.0f
        }

        val secondVector = FloatArray(laneSize + 2)
        for (i in secondVector.indices) {
            secondVector[i] = i * 1.0f + 3.0f
        }

        var sum  = 0.0
        for (i in 0 until laneSize + 2) {
            val diff = firstVector[i] - secondVector[i]
            sum += diff * diff
        }

        val distance = l2Distance.computeDistance(firstVector, secondVector)
        Assert.assertEquals(sum, distance, 0.0)
    }

    @Test
    fun testComputeDistanceWitLanesNonZeroOffsets() {
        val l2Distance = L2Distance()
        val laneSize = species.length()

        val firstVector = FloatArray(laneSize + 2 + 3 + 2)

        for (i in firstVector.indices) {
            firstVector[i] = i * 1.0f
        }

        val secondVector = FloatArray(laneSize + 2 + 2 + 1)
        for (i in secondVector.indices) {
            secondVector[i] = i * 1.0f + 3.0f
        }

        var sum  = 0.0
        for (i in 0 until laneSize + 2) {
            val diff = firstVector[i + 3] - secondVector[i + 2]
            sum += diff * diff
        }

        val distance = l2Distance.computeDistance(firstVector, 3, secondVector, 2,
            laneSize + 2)
        Assert.assertEquals(sum, distance, 0.0)
    }

    companion object {
        private val species: VectorSpecies<Float> = FloatVector.SPECIES_PREFERRED
    }
}