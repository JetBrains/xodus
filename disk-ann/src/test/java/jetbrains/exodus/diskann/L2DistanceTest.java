package jetbrains.exodus.diskann;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorSpecies;
import org.junit.Assert;
import org.junit.Test;

public class L2DistanceTest {
    private static final VectorSpecies<Float> species = FloatVector.SPECIES_PREFERRED;

    @Test
    public void testComputeDistanceNoLanes() {
        var l2Distance = new L2Distance();
        var firstVector = new float[]{1.0f, 2.0f, 3.0f};
        var secondVector = new float[]{4.0f, 5.0f, 6.0f};

        var distance = l2Distance.computeDistance(firstVector, secondVector);
        Assert.assertEquals(27.0, distance, 0.0);
    }

    @Test
    public void testComputeDistanceWithNonZeroOffsetsNoLanes() {
        var l2Distance = new L2Distance();
        var firstVector = new float[]{1.0f, 2.0f, 3.0f};
        var secondVector = new float[]{4.0f, 5.0f, 6.0f};
        var distance = l2Distance.computeDistance(firstVector, 1, secondVector,
                1, 1);
        Assert.assertEquals(9.0, distance, 0.0);
    }

    @Test
    public void testComputeDistanceWitLanes() {
        var l2Distance = new L2Distance();
        var laneSize = species.length();
        var firstVector = new float[laneSize + 2];

        for (var i = 0; i < firstVector.length; i++) {
            firstVector[i] = i * 1.0f;
        }

        var secondVector = new float[laneSize + 2];
        for (var i = 0; i < secondVector.length; i++) {
            secondVector[i] = i * 1.0f + 3.0f;
        }

        var sum = 0.0;
        for (var i = 0; i < laneSize + 2; i++) {
            var diff = firstVector[i] - secondVector[i];
            sum += diff * diff;
        }

        var distance = l2Distance.computeDistance(firstVector, secondVector);
        Assert.assertEquals(sum, distance, 0.0);
    }

    @Test
    public void testComputeDistanceWitLanesNonZeroOffsets() {
        var l2Distance = new L2Distance();
        var laneSize = species.length();

        var firstVector = new float[laneSize + 2 + 3 + 2];

        for (var i = 0; i < firstVector.length; i++) {
            firstVector[i] = i * 1.0f;
        }

        var secondVector = new float[laneSize + 2 + 2 + 1];
        for (var i = 0; i < secondVector.length; i++) {
            secondVector[i] = i * 1.0f + 3.0f;
        }

        var sum = 0.0;
        for (var i = 0; i < laneSize + 2; i++) {
            var diff = firstVector[i + 3] - secondVector[i + 2];
            sum += diff * diff;
        }

        var distance = l2Distance.computeDistance(firstVector, 3, secondVector, 2,
                laneSize + 2);
        Assert.assertEquals(sum, distance, 0.0);
    }

}
