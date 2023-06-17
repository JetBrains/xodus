package jetbrains.exodus.diskann;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public final class L2Distance implements DistanceFunction {
    private static final VectorSpecies<Float> species = FloatVector.SPECIES_PREFERRED;

    public double computeDistance(float[] firstVector, float[] secondVector) {
        var sumVector = FloatVector.zero(species);
        var index = 0;

        while (index < species.loopBound(firstVector.length)) {
            var first = FloatVector.fromArray(species, firstVector, index);
            var second = FloatVector.fromArray(species, secondVector, index);
            var diff = first.sub(second);
            var mul = diff.mul(diff);

            sumVector = sumVector.add(mul);
            index += species.length();
        }

        var sum = (double) sumVector.reduceLanes(VectorOperators.ADD);

        while (index < firstVector.length) {
            var diff = firstVector[index] - secondVector[index];
            sum += diff * diff;
            index++;
        }

        return sum;
    }

    public double computeDistance(
            float[] firstVector,
            int firstVectorFrom,
            float[] secondVector,
            int secondVectorFrom,
            int size
    ) {
        var sumVector = FloatVector.zero(species);
        var index = 0;

        while (index < species.loopBound(size)) {
            var first = FloatVector.fromArray(species, firstVector, index + firstVectorFrom);
            var second = FloatVector.fromArray(species, secondVector, index + secondVectorFrom);
            var diff = first.sub(second);
            var mul = diff.mul(diff);

            sumVector = sumVector.add(mul);
            index += species.length();
        }

        var sum = sumVector.reduceLanes(VectorOperators.ADD);

        while (index < size) {
            var diff = firstVector[index + firstVectorFrom] - secondVector[index + secondVectorFrom];
            sum += diff * diff;
            index++;
        }

        return sum;
    }
}
