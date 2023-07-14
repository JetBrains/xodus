package jetbrains.exodus.diskann;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

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
                                              final MemorySegment secondSegment, long secondSegmentOffset, int size) {
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
