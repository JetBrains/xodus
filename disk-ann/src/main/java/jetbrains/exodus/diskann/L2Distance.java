package jetbrains.exodus.diskann;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.util.Arrays;

public final class L2Distance {
    private static final int PREFERRED_SPECIES_LENGTH = FloatVector.SPECIES_PREFERRED.length();

    public static float computeL2Distance(final float[] firstVector, int firstVectorOffset,
                                          final float[] secondVector, int secondVectorOffset,
                                          int size) {
        return computeL2Distance(firstVector, firstVectorOffset, secondVector, secondVectorOffset, size,
                PREFERRED_SPECIES_LENGTH);
    }

    static float computeL2Distance(float[] firstVector, int firstVectorOffset, float[] secondVector,
                                   int secondVectorOffset, int size, int speciesLength) {
        var sum = 0.0f;
        var step = closestSIMDStep(speciesLength, size);

        if (step == 16) {
            var loopBound = FloatVector.SPECIES_512.loopBound(size);
            sum += computeL2Distance512(firstVector, firstVectorOffset, secondVector,
                    secondVectorOffset, loopBound);
            size -= loopBound;
            firstVectorOffset += loopBound;
            secondVectorOffset += loopBound;

            step = closestSIMDStep(8, size);
        }

        if (step == 8) {
            var loopBound = FloatVector.SPECIES_256.loopBound(size);
            sum += computeL2Distance256(firstVector, firstVectorOffset, secondVector,
                    secondVectorOffset, loopBound);
            size -= loopBound;

            firstVectorOffset += loopBound;
            secondVectorOffset += loopBound;
        }

        if (size > 0) {
            sum += computeL2Distance128(firstVector, firstVectorOffset, secondVector,
                    secondVectorOffset, size);
        }

        return sum;
    }

    public static float computeL2Distance(final MemorySegment firstSegment, long firstSegmentOffset,
                                          final float[] secondVector, int secondVectorOffset) {

        return computeL2Distance(firstSegment, firstSegmentOffset, secondVector, secondVectorOffset,
                PREFERRED_SPECIES_LENGTH);
    }

    static float computeL2Distance(MemorySegment firstSegment, long firstSegmentOffset, float[] secondVector,
                                   int secondVectorOffset, int speciesLength) {
        var size = secondVector.length - secondVectorOffset;
        var step = closestSIMDStep(speciesLength, size);
        var sum = 0.0f;

        if (step == 16) {
            var loopBound = FloatVector.SPECIES_512.loopBound(size);
            sum += computeL2Distance512(firstSegment, firstSegmentOffset, secondVector,
                    secondVectorOffset, loopBound);
            size -= loopBound;
            firstSegmentOffset += (long) loopBound * Float.BYTES;
            secondVectorOffset += loopBound;

            step = closestSIMDStep(8, size);
        }
        if (step == 8) {
            var loopBound = FloatVector.SPECIES_256.loopBound(size);
            sum += computeL2Distance256(firstSegment, firstSegmentOffset, secondVector,
                    secondVectorOffset, loopBound);
            size -= loopBound;

            firstSegmentOffset += (long) loopBound * Float.BYTES;
            secondVectorOffset += loopBound;
        }

        if (size > 0) {
            sum += computeL2Distance128(firstSegment, firstSegmentOffset, secondVector,
                    secondVectorOffset, size);
        }

        return sum;
    }

    public static float computeL2Distance(final MemorySegment firstSegment, long firstSegmentOffset,
                                          final MemorySegment secondSegment, long secondSegmentOffset, int size) {
        return computeL2Distance(firstSegment, firstSegmentOffset, secondSegment, secondSegmentOffset, size,
                PREFERRED_SPECIES_LENGTH);
    }

    public static void computeL2Distance(final MemorySegment originSegment, long originSegmentOffset,
                                         final MemorySegment firstSegment, long firstSegmentOffset,
                                         final MemorySegment secondSegment, long secondSegmentOffset,
                                         final MemorySegment thirdSegment, long thirdSegmentOffset,
                                         final MemorySegment fourthSegment, long fourthSegmentOffset,
                                         int size, float[] result) {
        computeL2Distance(originSegment, originSegmentOffset, firstSegment, firstSegmentOffset,
                secondSegment, secondSegmentOffset, thirdSegment, thirdSegmentOffset,
                fourthSegment, fourthSegmentOffset, size, result, PREFERRED_SPECIES_LENGTH);
    }

    static float computeL2Distance(MemorySegment firstSegment, long firstSegmentOffset, MemorySegment secondSegment,
                                   long secondSegmentOffset, int size, int speciesLength) {
        var sum = 0.0f;
        var step = closestSIMDStep(speciesLength, size);

        if (step == 16) {
            var loopBound = FloatVector.SPECIES_512.loopBound(size);

            sum += computeL2Distance512(firstSegment, firstSegmentOffset, secondSegment,
                    secondSegmentOffset, loopBound);
            size -= loopBound;
            firstSegmentOffset += (long) loopBound * Float.BYTES;
            secondSegmentOffset += (long) loopBound * Float.BYTES;

            step = closestSIMDStep(8, size);
        }
        if (step == 8) {
            var loopBound = FloatVector.SPECIES_256.loopBound(size);
            sum += computeL2Distance256(firstSegment, firstSegmentOffset, secondSegment,
                    secondSegmentOffset, loopBound);
            size -= loopBound;

            firstSegmentOffset += (long) loopBound * Float.BYTES;
            secondSegmentOffset += (long) loopBound * Float.BYTES;
        }

        if (size > 0) {
            sum += computeL2Distance128(firstSegment, firstSegmentOffset, secondSegment,
                    secondSegmentOffset, size);
        }

        return sum;
    }

    static void computeL2Distance(MemorySegment originSegment, long originSegmentOffset,
                                  MemorySegment firstSegment, long firstSegmentOffset,
                                  MemorySegment secondSegment, long secondSegmentOffset,
                                  MemorySegment thirdSegment, long thirdSegmentOffset,
                                  MemorySegment fourthSegment, long fourthSegmentOffset,
                                  int size, float[] result,
                                  int speciesLength) {
        Arrays.fill(result, 0.0f);

        var step = closestSIMDStep(speciesLength, size);

        if (step == 16) {
            var loopBound = FloatVector.SPECIES_512.loopBound(size);

            computeL2Distance512(originSegment, originSegmentOffset, firstSegment,
                    firstSegmentOffset, secondSegment, secondSegmentOffset,
                    thirdSegment, thirdSegmentOffset, fourthSegment, fourthSegmentOffset,
                    result, loopBound);

            size -= loopBound;

            originSegmentOffset += (long) loopBound * Float.BYTES;
            firstSegmentOffset += (long) loopBound * Float.BYTES;
            secondSegmentOffset += (long) loopBound * Float.BYTES;
            thirdSegmentOffset += (long) loopBound * Float.BYTES;
            fourthSegmentOffset += (long) loopBound * Float.BYTES;

            step = closestSIMDStep(8, size);
        }
        if (step == 8) {
            var loopBound = FloatVector.SPECIES_256.loopBound(size);
            computeL2Distance256(originSegment, originSegmentOffset, firstSegment,
                    firstSegmentOffset, secondSegment, secondSegmentOffset,
                    thirdSegment, thirdSegmentOffset, fourthSegment, fourthSegmentOffset,
                    result, loopBound);

            size -= loopBound;

            originSegmentOffset += (long) loopBound * Float.BYTES;
            firstSegmentOffset += (long) loopBound * Float.BYTES;
            secondSegmentOffset += (long) loopBound * Float.BYTES;
            thirdSegmentOffset += (long) loopBound * Float.BYTES;
            fourthSegmentOffset += (long) loopBound * Float.BYTES;
        }

        if (size > 0) {
            computeL2Distance128(originSegment, originSegmentOffset, firstSegment,
                    firstSegmentOffset,
                    secondSegment, secondSegmentOffset,
                    thirdSegment, thirdSegmentOffset,
                    fourthSegment, fourthSegmentOffset,
                    result, size);
        }
    }


    private static int closestSIMDStep(int step, int size) {
        return Integer.highestOneBit(Math.min(step, size));
    }

    private static float computeL2Distance512(final MemorySegment firstSegment, long firstSegmentOffset,
                                              final MemorySegment secondSegment, long secondSegmentOffset, int size) {
        assert size == FloatVector.SPECIES_512.loopBound(size);

        var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                firstSegmentOffset, ByteOrder.nativeOrder());
        var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, secondSegment,
                secondSegmentOffset, ByteOrder.nativeOrder());

        var diffVector = first.sub(second);
        var sumVector = diffVector.mul(diffVector);

        firstSegmentOffset += 16 * Float.BYTES;
        secondSegmentOffset += 16 * Float.BYTES;

        for (var index = 16; index < size; index += 16, firstSegmentOffset += 16 * Float.BYTES,
                secondSegmentOffset += 16 * Float.BYTES) {
            first = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            second = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder());
            diffVector = first.sub(second);

            sumVector = diffVector.fma(diffVector, sumVector);
        }

        return sumVector.reduceLanes(VectorOperators.ADD);
    }

    private static void computeL2Distance512(final MemorySegment originalSegment, long originalSegmentOffset,
                                             final MemorySegment firstSegment, long firstSegmentOffset,
                                             final MemorySegment secondSegment, long secondSegmentOffset,
                                             final MemorySegment thirdSegment, long thirdSegmentOffset,
                                             final MemorySegment fourthSegment, long fourthSegmentOffset,
                                             float[] result,
                                             int size) {
        assert size == FloatVector.SPECIES_512.loopBound(size);

        var original = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, originalSegment,
                originalSegmentOffset, ByteOrder.nativeOrder());

        var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                firstSegmentOffset, ByteOrder.nativeOrder());
        var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, secondSegment,
                secondSegmentOffset, ByteOrder.nativeOrder());
        var third = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, thirdSegment,
                thirdSegmentOffset, ByteOrder.nativeOrder());
        var fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, fourthSegment,
                fourthSegmentOffset, ByteOrder.nativeOrder());

        var diffVector_1 = original.sub(first);
        var diffVector_2 = original.sub(second);
        var diffVector_3 = original.sub(third);
        var diffVector_4 = original.sub(fourth);

        var sumVector_1 = diffVector_1.mul(diffVector_1);
        var sumVector_2 = diffVector_2.mul(diffVector_2);
        var sumVector_3 = diffVector_3.mul(diffVector_3);
        var sumVector_4 = diffVector_4.mul(diffVector_4);

        originalSegmentOffset += 16 * Float.BYTES;

        firstSegmentOffset += 16 * Float.BYTES;
        secondSegmentOffset += 16 * Float.BYTES;
        thirdSegmentOffset += 16 * Float.BYTES;
        fourthSegmentOffset += 16 * Float.BYTES;

        for (var index = 16; index < size; index += 16, originalSegmentOffset += 16 * Float.BYTES,
                firstSegmentOffset += 16 * Float.BYTES, secondSegmentOffset += 16 * Float.BYTES,
                thirdSegmentOffset += 16 * Float.BYTES, fourthSegmentOffset += 16 * Float.BYTES) {
            original = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, originalSegment,
                    originalSegmentOffset, ByteOrder.nativeOrder());
            first = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            second = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder());
            third = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, thirdSegment,
                    thirdSegmentOffset, ByteOrder.nativeOrder());
            fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, fourthSegment,
                    fourthSegmentOffset, ByteOrder.nativeOrder());

            diffVector_1 = original.sub(first);
            diffVector_2 = original.sub(second);
            diffVector_3 = original.sub(third);
            diffVector_4 = original.sub(fourth);

            sumVector_1 = diffVector_1.fma(diffVector_1, sumVector_1);
            sumVector_2 = diffVector_2.fma(diffVector_2, sumVector_2);
            sumVector_3 = diffVector_3.fma(diffVector_3, sumVector_3);
            sumVector_4 = diffVector_4.fma(diffVector_4, sumVector_4);
        }

        result[0] = sumVector_1.reduceLanes(VectorOperators.ADD);
        result[1] = sumVector_2.reduceLanes(VectorOperators.ADD);
        result[2] = sumVector_3.reduceLanes(VectorOperators.ADD);
        result[3] = sumVector_4.reduceLanes(VectorOperators.ADD);
    }


    private static float computeL2Distance512(final float[] firstVector, int firstVectorOffset,
                                              final float[] secondVector, int secondVectorOffset, int size) {
        var step = 16;
        assert size == FloatVector.SPECIES_512.loopBound(size);

        var first = FloatVector.fromArray(FloatVector.SPECIES_512, firstVector,
                firstVectorOffset);
        var second = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                secondVectorOffset);

        var diffVector = first.sub(second);
        var sumVector = diffVector.mul(diffVector);

        firstVectorOffset += step;
        secondVectorOffset += step;

        for (var index = step; index < size; index += step, firstVectorOffset += step,
                secondVectorOffset += step) {
            first = FloatVector.fromArray(FloatVector.SPECIES_512, firstVector,
                    firstVectorOffset);
            second = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                    secondVectorOffset);
            diffVector = first.sub(second);

            sumVector = diffVector.fma(diffVector, sumVector);
        }

        return sumVector.reduceLanes(VectorOperators.ADD);
    }

    private static float computeL2Distance512(final MemorySegment firstSegment, long firstSegmentOffset,
                                              final float[] secondVector, int secondVectorOffset, int size) {
        var step = 16;
        var segmentStep = 16 * Float.BYTES;

        assert size == FloatVector.SPECIES_128.loopBound(size);

        var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                firstSegmentOffset, ByteOrder.nativeOrder());
        var second = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                secondVectorOffset);

        var diffVector = first.sub(second);
        var sumVector = diffVector.mul(diffVector);

        firstSegmentOffset += segmentStep;
        secondVectorOffset += step;

        for (var index = step; index < size; index += step, firstSegmentOffset += segmentStep,
                secondVectorOffset += step) {
            first = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            second = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                    secondVectorOffset);
            diffVector = first.sub(second);

            sumVector = diffVector.fma(diffVector, sumVector);
        }

        return sumVector.reduceLanes(VectorOperators.ADD);
    }


    private static float computeL2Distance256(final MemorySegment firstSegment, long firstSegmentOffset,
                                              final MemorySegment secondSegment, long secondSegmentOffset,
                                              int size) {
        var step = 8;
        var segmentStep = 8 * Float.BYTES;

        assert size == FloatVector.SPECIES_256.loopBound(size);

        var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                firstSegmentOffset, ByteOrder.nativeOrder());
        var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, secondSegment,
                secondSegmentOffset, ByteOrder.nativeOrder());
        var diffVector = first.sub(second);
        var sumVector = diffVector.mul(diffVector);

        firstSegmentOffset += segmentStep;
        secondSegmentOffset += segmentStep;

        for (var index = step; index < size; index += step, firstSegmentOffset += segmentStep,
                secondSegmentOffset += segmentStep) {
            first = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            second = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder());
            diffVector = first.sub(second);

            sumVector = diffVector.fma(diffVector, sumVector);
        }

        return sumVector.reduceLanes(VectorOperators.ADD);
    }

    private static void computeL2Distance256(final MemorySegment originalSegment, long orginalSegmentOffset,
                                             final MemorySegment firstSegment, long firstSegmentOffset,
                                             final MemorySegment secondSegment, long secondSegmentOffset,
                                             final MemorySegment thirdSegment, long thirdSegmentOffset,
                                             final MemorySegment fourthSegment, long fourthSegmentOffset,
                                             float[] result,
                                             int size) {
        var step = 8;
        var segmentStep = 8 * Float.BYTES;

        assert size == FloatVector.SPECIES_256.loopBound(size);

        var original = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, originalSegment,
                orginalSegmentOffset, ByteOrder.nativeOrder());

        var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                firstSegmentOffset, ByteOrder.nativeOrder());
        var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, secondSegment,
                secondSegmentOffset, ByteOrder.nativeOrder());
        var third = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, thirdSegment,
                thirdSegmentOffset, ByteOrder.nativeOrder());
        var fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, fourthSegment,
                fourthSegmentOffset, ByteOrder.nativeOrder());

        var diffVector_1 = original.sub(first);
        var diffVector_2 = original.sub(second);
        var diffVector_3 = original.sub(third);
        var diffVector_4 = original.sub(fourth);

        var sumVector_1 = diffVector_1.mul(diffVector_1);
        var sumVector_2 = diffVector_2.mul(diffVector_2);
        var sumVector_3 = diffVector_3.mul(diffVector_3);
        var sumVector_4 = diffVector_4.mul(diffVector_4);

        orginalSegmentOffset += segmentStep;
        firstSegmentOffset += segmentStep;
        thirdSegmentOffset += segmentStep;
        secondSegmentOffset += segmentStep;
        fourthSegmentOffset += segmentStep;

        for (var index = step; index < size; index += step, orginalSegmentOffset += segmentStep,
                firstSegmentOffset += segmentStep, secondSegmentOffset += segmentStep, thirdSegmentOffset += segmentStep,
                fourthSegmentOffset += segmentStep) {
            original = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, originalSegment,
                    orginalSegmentOffset, ByteOrder.nativeOrder());
            first = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            second = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder());
            third = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, thirdSegment,
                    thirdSegmentOffset, ByteOrder.nativeOrder());
            fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, fourthSegment,
                    fourthSegmentOffset, ByteOrder.nativeOrder());

            diffVector_1 = original.sub(first);
            diffVector_2 = original.sub(second);
            diffVector_3 = original.sub(third);
            diffVector_4 = original.sub(fourth);

            sumVector_1 = diffVector_1.fma(diffVector_1, sumVector_1);
            sumVector_2 = diffVector_2.fma(diffVector_2, sumVector_2);
            sumVector_3 = diffVector_3.fma(diffVector_3, sumVector_3);
            sumVector_4 = diffVector_4.fma(diffVector_4, sumVector_4);
        }

        result[0] += sumVector_1.reduceLanes(VectorOperators.ADD);
        result[1] += sumVector_2.reduceLanes(VectorOperators.ADD);
        result[2] += sumVector_3.reduceLanes(VectorOperators.ADD);
        result[3] += sumVector_4.reduceLanes(VectorOperators.ADD);
    }

    private static float computeL2Distance256(final MemorySegment firstSegment, long firstSegmentOffset,
                                              final float[] secondVector, int secondVectorOffset, int size) {
        var step = 8;
        var segmentStep = 8 * Float.BYTES;

        assert size == FloatVector.SPECIES_256.loopBound(size);

        var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                firstSegmentOffset, ByteOrder.nativeOrder());
        var second = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                secondVectorOffset);
        var diffVector = first.sub(second);
        var sumVector = diffVector.mul(diffVector);

        firstSegmentOffset += segmentStep;
        secondVectorOffset += step;

        for (var index = step; index < size; index += step, firstSegmentOffset += segmentStep,
                secondVectorOffset += step) {
            first = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            second = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                    secondVectorOffset);
            diffVector = first.sub(second);

            sumVector = diffVector.fma(diffVector, sumVector);
        }

        return sumVector.reduceLanes(VectorOperators.ADD);
    }

    private static float computeL2Distance256(final float[] firstVector, int firstVectorOffset,
                                              final float[] secondVector, int secondVectorOffset, int size) {
        var step = 8;
        assert size == FloatVector.SPECIES_256.loopBound(size);

        var first = FloatVector.fromArray(FloatVector.SPECIES_256, firstVector,
                firstVectorOffset);
        var second = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                secondVectorOffset);
        var diffVector = first.sub(second);
        var sumVector = diffVector.mul(diffVector);

        firstVectorOffset += step;
        secondVectorOffset += step;

        for (var index = step; index < size; index += step, firstVectorOffset += step,
                secondVectorOffset += step) {
            first = FloatVector.fromArray(FloatVector.SPECIES_256, firstVector,
                    firstVectorOffset);
            second = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                    secondVectorOffset);
            diffVector = first.sub(second);

            sumVector = diffVector.fma(diffVector, sumVector);
        }

        return sumVector.reduceLanes(VectorOperators.ADD);
    }

    private static float computeL2Distance128(final MemorySegment firstSegment, long firstSegmentOffset,
                                              final MemorySegment secondSegment, long secondSegmentOffset,
                                              int size) {
        var step = 4;
        var index = 0;
        var sum = 0.0f;

        if (size >= step) {
            var segmentStep = 4 * Float.BYTES;

            var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder());
            var diffVector = first.sub(second);
            var sumVector = diffVector.mul(diffVector);

            firstSegmentOffset += segmentStep;
            secondSegmentOffset += segmentStep;

            index = step;

            var loopBound = FloatVector.SPECIES_128.loopBound(size);

            for (; index < loopBound; index += step, firstSegmentOffset += segmentStep,
                    secondSegmentOffset += segmentStep) {
                first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                        firstSegmentOffset, ByteOrder.nativeOrder());
                second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment,
                        secondSegmentOffset, ByteOrder.nativeOrder());
                diffVector = first.sub(second);

                sumVector = diffVector.fma(diffVector, sumVector);
            }

            if (index < size) {
                var mask = FloatVector.SPECIES_128.indexInRange(index, size);

                first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                        firstSegmentOffset, ByteOrder.nativeOrder(), mask);
                second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment,
                        secondSegmentOffset, ByteOrder.nativeOrder(), mask);
                diffVector = first.sub(second, mask);

                sumVector = diffVector.mul(diffVector, mask).add(sumVector);
            }

            sum = sumVector.reduceLanes(VectorOperators.ADD);
        } else {
            var mask = FloatVector.SPECIES_128.indexInRange(index, size);

            var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder(), mask);
            var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder(), mask);
            var diffVector = first.sub(second, mask);
            var sumVector = diffVector.mul(diffVector, mask);

            sum = sumVector.reduceLanes(VectorOperators.ADD, mask);
        }

        return sum;
    }

    private static void computeL2Distance128(final MemorySegment originalSegment, long originalSegmentOffset,
                                             final MemorySegment firstSegment, long firstSegmentOffset,
                                             final MemorySegment secondSegment, long secondSegmentOffset,
                                             final MemorySegment thirdSegment, long thirdSegmentOffset,
                                             final MemorySegment fourthSegment, long fourthSegmentOffset,
                                             float[] result,
                                             int size) {
        var step = 4;
        var index = 0;

        if (size >= step) {
            var segmentStep = 4 * Float.BYTES;

            var original = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, originalSegment,
                    originalSegmentOffset, ByteOrder.nativeOrder());
            var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder());
            var third = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, thirdSegment,
                    thirdSegmentOffset, ByteOrder.nativeOrder());
            var fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, fourthSegment,
                    fourthSegmentOffset, ByteOrder.nativeOrder());

            var diffVector_1 = original.sub(first);
            var diffVector_2 = original.sub(second);
            var diffVector_3 = original.sub(third);
            var diffVector_4 = original.sub(fourth);

            var sumVector_1 = diffVector_1.mul(diffVector_1);
            var sumVector_2 = diffVector_2.mul(diffVector_2);
            var sumVector_3 = diffVector_3.mul(diffVector_3);
            var sumVector_4 = diffVector_4.mul(diffVector_4);

            originalSegmentOffset += segmentStep;
            firstSegmentOffset += segmentStep;
            secondSegmentOffset += segmentStep;
            thirdSegmentOffset += segmentStep;
            fourthSegmentOffset += segmentStep;

            index = step;

            var loopBound = FloatVector.SPECIES_128.loopBound(size);

            for (; index < loopBound; index += step, originalSegmentOffset += segmentStep,
                    firstSegmentOffset += segmentStep, secondSegmentOffset += segmentStep,
                    thirdSegmentOffset += segmentStep, fourthSegmentOffset += segmentStep) {

                original = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, originalSegment,
                        originalSegmentOffset, ByteOrder.nativeOrder());
                first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                        firstSegmentOffset, ByteOrder.nativeOrder());
                second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment,
                        secondSegmentOffset, ByteOrder.nativeOrder());
                third = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, thirdSegment,
                        thirdSegmentOffset, ByteOrder.nativeOrder());
                fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, fourthSegment,
                        fourthSegmentOffset, ByteOrder.nativeOrder());


                diffVector_1 = original.sub(first);
                diffVector_2 = original.sub(second);
                diffVector_3 = original.sub(third);
                diffVector_4 = original.sub(fourth);

                sumVector_1 = diffVector_1.fma(diffVector_1, sumVector_1);
                sumVector_2 = diffVector_2.fma(diffVector_2, sumVector_2);
                sumVector_3 = diffVector_3.fma(diffVector_3, sumVector_3);
                sumVector_4 = diffVector_4.fma(diffVector_4, sumVector_4);
            }

            if (index < size) {
                var mask = FloatVector.SPECIES_128.indexInRange(index, size);

                original = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, originalSegment,
                        originalSegmentOffset, ByteOrder.nativeOrder(), mask);
                first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                        firstSegmentOffset, ByteOrder.nativeOrder(), mask);
                second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment, secondSegmentOffset,
                        ByteOrder.nativeOrder(), mask);
                third = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, thirdSegment, thirdSegmentOffset,
                        ByteOrder.nativeOrder(), mask);
                fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, fourthSegment, fourthSegmentOffset,
                        ByteOrder.nativeOrder(), mask);


                diffVector_1 = original.sub(first, mask);
                diffVector_2 = original.sub(second, mask);
                diffVector_3 = original.sub(third, mask);
                diffVector_4 = original.sub(fourth, mask);

                sumVector_1 = diffVector_1.mul(diffVector_1, mask).add(sumVector_1);
                sumVector_2 = diffVector_2.mul(diffVector_2, mask).add(sumVector_2);
                sumVector_3 = diffVector_3.mul(diffVector_3, mask).add(sumVector_3);
                sumVector_4 = diffVector_4.mul(diffVector_4, mask).add(sumVector_4);
            }

            result[0] += sumVector_1.reduceLanes(VectorOperators.ADD);
            result[1] += sumVector_2.reduceLanes(VectorOperators.ADD);
            result[2] += sumVector_3.reduceLanes(VectorOperators.ADD);
            result[3] += sumVector_4.reduceLanes(VectorOperators.ADD);
        } else {
            var mask = FloatVector.SPECIES_128.indexInRange(index, size);

            var original = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, originalSegment,
                    originalSegmentOffset, ByteOrder.nativeOrder(), mask);
            var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder(), mask);
            var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder(), mask);
            var third = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, thirdSegment, thirdSegmentOffset,
                    ByteOrder.nativeOrder(), mask);
            var fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, fourthSegment, fourthSegmentOffset,
                    ByteOrder.nativeOrder(), mask);


            var diffVector_1 = original.sub(first, mask);
            var diffVector_2 = original.sub(second, mask);
            var diffVector_3 = original.sub(third, mask);
            var diffVector_4 = original.sub(fourth, mask);

            var sumVector_1 = diffVector_1.mul(diffVector_1, mask);
            var sumVector_2 = diffVector_2.mul(diffVector_2, mask);
            var sumVector_3 = diffVector_3.mul(diffVector_3, mask);
            var sumVector_4 = diffVector_4.mul(diffVector_4, mask);

            result[0] += sumVector_1.reduceLanes(VectorOperators.ADD, mask);
            result[1] += sumVector_2.reduceLanes(VectorOperators.ADD, mask);
            result[2] += sumVector_3.reduceLanes(VectorOperators.ADD, mask);
            result[3] += sumVector_4.reduceLanes(VectorOperators.ADD, mask);
        }
    }


    private static float computeL2Distance128(final MemorySegment firstSegment, long firstSegmentOffset,
                                              final float[] secondVector, int secondVectorOffset,
                                              int size) {
        var step = 4;
        var index = 0;
        var sum = 0.0f;

        if (size >= step) {
            var segmentStep = 4 * Float.BYTES;

            var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            var second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                    secondVectorOffset);
            var diffVector = first.sub(second);
            var sumVector = diffVector.mul(diffVector);

            firstSegmentOffset += segmentStep;
            secondVectorOffset += step;

            index = step;

            var loopBound = FloatVector.SPECIES_128.loopBound(size);

            for (; index < loopBound; index += step, firstSegmentOffset += segmentStep,
                    secondVectorOffset += step) {
                first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                        firstSegmentOffset, ByteOrder.nativeOrder());
                second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                        secondVectorOffset);
                diffVector = first.sub(second);

                sumVector = diffVector.fma(diffVector, sumVector);
            }

            if (index < size) {
                var mask = FloatVector.SPECIES_128.indexInRange(index, size);

                first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                        firstSegmentOffset, ByteOrder.nativeOrder(), mask);
                second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                        secondVectorOffset, mask);
                diffVector = first.sub(second, mask);

                sumVector = diffVector.mul(diffVector, mask).add(sumVector);
            }

            sum = sumVector.reduceLanes(VectorOperators.ADD);
        } else {
            var mask = FloatVector.SPECIES_128.indexInRange(index, size);

            var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder(), mask);
            var second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                    secondVectorOffset, mask);
            var diffVector = first.sub(second, mask);
            var sumVector = diffVector.mul(diffVector, mask);

            sum = sumVector.reduceLanes(VectorOperators.ADD, mask);
        }

        return sum;
    }

    private static float computeL2Distance128(final float[] firstVector, int firstVectorOffset,
                                              final float[] secondVector, int secondVectorOffset, int size) {
        var step = 4;
        var index = 0;
        var sum = 0.0f;

        if (size >= step) {
            var first = FloatVector.fromArray(FloatVector.SPECIES_128, firstVector,
                    firstVectorOffset);
            var second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                    secondVectorOffset);
            var diffVector = first.sub(second);
            var sumVector = diffVector.mul(diffVector);

            index = step;
            firstVectorOffset += step;
            secondVectorOffset += step;

            var loopBound = FloatVector.SPECIES_128.loopBound(size);

            for (; index < loopBound; index += step, firstVectorOffset += step,
                    secondVectorOffset += step) {
                first = FloatVector.fromArray(FloatVector.SPECIES_128, firstVector,
                        firstVectorOffset);
                second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                        secondVectorOffset);
                diffVector = first.sub(second);

                sumVector = diffVector.fma(diffVector, sumVector);
            }

            if (index < size) {
                var mask = FloatVector.SPECIES_128.indexInRange(index, size);

                first = FloatVector.fromArray(FloatVector.SPECIES_128, firstVector,
                        firstVectorOffset, mask);
                second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                        secondVectorOffset, mask);
                diffVector = first.sub(second, mask);

                sumVector = diffVector.mul(diffVector, mask).add(sumVector);
            }

            sum = sumVector.reduceLanes(VectorOperators.ADD);
        } else {
            var mask = FloatVector.SPECIES_128.indexInRange(index, size);

            var first = FloatVector.fromArray(FloatVector.SPECIES_128, firstVector,
                    firstVectorOffset, mask);
            var second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                    secondVectorOffset, mask);
            var diffVector = first.sub(second, mask);
            var sumVector = diffVector.mul(diffVector, mask);

            sum = sumVector.reduceLanes(VectorOperators.ADD, mask);
        }

        return sum;
    }
}
