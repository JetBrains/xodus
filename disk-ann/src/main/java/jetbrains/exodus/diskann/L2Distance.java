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
            var batchStep = 4 * step;
            if (loopBound >= batchStep) {
                var batchLoopBound = loopBound & (-batchStep);

                sum += computeL2Distance512Batch(firstVector, firstVectorOffset, secondVector,
                        secondVectorOffset, batchLoopBound);
                size -= batchLoopBound;
                firstVectorOffset += batchLoopBound;
                secondVectorOffset += batchLoopBound;
                loopBound -= batchLoopBound;
            }

            if (loopBound > 0) {
                sum += computeL2Distance512(firstVector, firstVectorOffset, secondVector,
                        secondVectorOffset, loopBound);
                size -= loopBound;
                firstVectorOffset += loopBound;
                secondVectorOffset += loopBound;
            }

            step = closestSIMDStep(8, size);
        }

        if (step == 8) {
            var loopBound = FloatVector.SPECIES_256.loopBound(size);
            var batchStep = 4 * step;
            if (loopBound >= batchStep) {
                var batchLoopBound = loopBound & (-batchStep);

                sum += computeL2Distance256Batch(firstVector, firstVectorOffset, secondVector,
                        secondVectorOffset, batchLoopBound);
                size -= batchLoopBound;
                firstVectorOffset += batchLoopBound;
                secondVectorOffset += batchLoopBound;
                loopBound -= batchLoopBound;
            }

            if (loopBound > 0) {
                sum += computeL2Distance256(firstVector, firstVectorOffset, secondVector,
                        secondVectorOffset, loopBound);
                size -= loopBound;

                firstVectorOffset += loopBound;
                secondVectorOffset += loopBound;
            }
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

    static float computeL2Distance(MemorySegment firstSegment, long firstSegmentOffset, MemorySegment secondSegment, long secondSegmentOffset, int size, int speciesLength) {
        var sum = 0.0f;
        var step = closestSIMDStep(speciesLength, size);

        if (step == 16) {
            var loopBound = FloatVector.SPECIES_512.loopBound(size);
            var batchStep = 4 * step;
            if (loopBound >= batchStep) {
                var batchLoopBound = loopBound & (-batchStep);

                sum += computeL2Distance512Batch(firstSegment, firstSegmentOffset, secondSegment,
                        secondSegmentOffset, batchLoopBound);
                size -= batchLoopBound;
                firstSegmentOffset += (long) batchLoopBound * Float.BYTES;
                secondSegmentOffset += (long) batchLoopBound * Float.BYTES;
                loopBound -= batchLoopBound;
            }

            if (loopBound > 0) {
                sum += computeL2Distance512(firstSegment, firstSegmentOffset, secondSegment,
                        secondSegmentOffset, loopBound);
                size -= loopBound;
                firstSegmentOffset += (long) loopBound * Float.BYTES;
                secondSegmentOffset += (long) loopBound * Float.BYTES;
            }
        }
        if (step == 8) {
            var loopBound = FloatVector.SPECIES_256.loopBound(size);
            var batchStep = 4 * step;
            if (loopBound >= batchStep) {
                var batchLoopBound = loopBound & (-batchStep);

                sum += computeL2Distance256Batch(firstSegment, firstSegmentOffset, secondSegment,
                        secondSegmentOffset, batchLoopBound);
                size -= batchLoopBound;

                firstSegmentOffset += (long) batchLoopBound * Float.BYTES;
                secondSegmentOffset += (long) batchLoopBound * Float.BYTES;
                loopBound -= batchLoopBound;
            }

            if (loopBound > 0) {
                sum += computeL2Distance256(firstSegment, firstSegmentOffset, secondSegment,
                        secondSegmentOffset, loopBound);
                size -= loopBound;

                firstSegmentOffset += (long) loopBound * Float.BYTES;
                secondSegmentOffset += (long) loopBound * Float.BYTES;
            }
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

    private static float computeL2Distance512Batch(final MemorySegment firstSegment, long firstSegmentOffset,
                                                   final MemorySegment secondSegment, long secondSegmentOffset, int size) {
        assert size == FloatVector.SPECIES_512.loopBound(size);

        var firstSegmentOffset_1 = firstSegmentOffset;
        var firstSegmentOffset_2 = firstSegmentOffset + 16 * Float.BYTES;
        var firstSegmentOffset_3 = firstSegmentOffset + 32 * Float.BYTES;
        var firstSegmentOffset_4 = firstSegmentOffset + 48 * Float.BYTES;

        var secondSegmentOffset_1 = secondSegmentOffset;
        var secondSegmentOffset_2 = secondSegmentOffset + 16 * Float.BYTES;
        var secondSegmentOffset_3 = secondSegmentOffset + 32 * Float.BYTES;
        var secondSegmentOffset_4 = secondSegmentOffset + 48 * Float.BYTES;

        var first_1 = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                firstSegmentOffset_1, ByteOrder.nativeOrder());
        var first_2 = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                firstSegmentOffset_2, ByteOrder.nativeOrder());
        var first_3 = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                firstSegmentOffset_3, ByteOrder.nativeOrder());
        var first_4 = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                firstSegmentOffset_4, ByteOrder.nativeOrder());

        var second_1 = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, secondSegment,
                secondSegmentOffset_1, ByteOrder.nativeOrder());
        var second_2 = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, secondSegment,
                secondSegmentOffset_2, ByteOrder.nativeOrder());
        var second_3 = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, secondSegment,
                secondSegmentOffset_3, ByteOrder.nativeOrder());
        var second_4 = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, secondSegment,
                secondSegmentOffset_4, ByteOrder.nativeOrder());

        var diffVector_1 = first_1.sub(second_1);
        var diffVector_2 = first_2.sub(second_2);
        var diffVector_3 = first_3.sub(second_3);
        var diffVector_4 = first_4.sub(second_4);

        var sumVector_1 = diffVector_1.mul(diffVector_1);
        var sumVector_2 = diffVector_2.mul(diffVector_2);
        var sumVector_3 = diffVector_3.mul(diffVector_3);
        var sumVector_4 = diffVector_4.mul(diffVector_4);

        firstSegmentOffset_1 = firstSegmentOffset_1 + 64 * Float.BYTES;
        firstSegmentOffset_2 = firstSegmentOffset_2 + 64 * Float.BYTES;
        firstSegmentOffset_3 = firstSegmentOffset_3 + 64 * Float.BYTES;
        firstSegmentOffset_4 = firstSegmentOffset_4 + 64 * Float.BYTES;

        secondSegmentOffset_1 = secondSegmentOffset_1 + 64 * Float.BYTES;
        secondSegmentOffset_2 = secondSegmentOffset_2 + 64 * Float.BYTES;
        secondSegmentOffset_3 = secondSegmentOffset_3 + 64 * Float.BYTES;
        secondSegmentOffset_4 = secondSegmentOffset_4 + 64 * Float.BYTES;

        for (var index = 64; index < size; index += 64,
                firstSegmentOffset_1 += 64 * Float.BYTES, firstSegmentOffset_2 += 64 * Float.BYTES,
                firstSegmentOffset_3 += 64 * Float.BYTES, firstSegmentOffset_4 += 64 * Float.BYTES,
                secondSegmentOffset_1 += 64 * Float.BYTES, secondSegmentOffset_2 += 64 * Float.BYTES,
                secondSegmentOffset_3 += 64 * Float.BYTES, secondSegmentOffset_4 += 64 * Float.BYTES) {
            first_1 = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                    firstSegmentOffset_1, ByteOrder.nativeOrder());
            first_2 = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                    firstSegmentOffset_2, ByteOrder.nativeOrder());
            first_3 = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                    firstSegmentOffset_3, ByteOrder.nativeOrder());
            first_4 = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                    firstSegmentOffset_4, ByteOrder.nativeOrder());

            second_1 = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, secondSegment,
                    secondSegmentOffset_1, ByteOrder.nativeOrder());
            second_2 = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, secondSegment,
                    secondSegmentOffset_2, ByteOrder.nativeOrder());
            second_3 = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, secondSegment,
                    secondSegmentOffset_3, ByteOrder.nativeOrder());
            second_4 = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, secondSegment,
                    secondSegmentOffset_4, ByteOrder.nativeOrder());

            diffVector_1 = first_1.sub(second_1);
            diffVector_2 = first_2.sub(second_2);
            diffVector_3 = first_3.sub(second_3);
            diffVector_4 = first_4.sub(second_4);

            sumVector_1 = diffVector_1.fma(diffVector_1, sumVector_1);
            sumVector_2 = diffVector_2.fma(diffVector_2, sumVector_2);
            sumVector_3 = diffVector_3.fma(diffVector_3, sumVector_3);
            sumVector_4 = diffVector_4.fma(diffVector_4, sumVector_4);
        }

        return sumVector_1.add(sumVector_2).add(sumVector_3).add(sumVector_4).reduceLanes(VectorOperators.ADD);
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

    private static float computeL2Distance512Batch(final float[] firstVector, int firstVectorOffset,
                                                   final float[] secondVector, int secondVectorOffset, int size) {
        assert size == FloatVector.SPECIES_512.loopBound(size);

        var firstVectorOffset_1 = firstVectorOffset;
        var firstVectorOffset_2 = firstVectorOffset + 16;
        var firstVectorOffset_3 = firstVectorOffset + 32;
        var firstVectorOffset_4 = firstVectorOffset + 48;

        var secondVectorOffset_1 = secondVectorOffset;
        var secondVectorOffset_2 = secondVectorOffset + 16;
        var secondVectorOffset_3 = secondVectorOffset + 32;
        var secondVectorOffset_4 = secondVectorOffset + 48;

        var first_1 = FloatVector.fromArray(FloatVector.SPECIES_512, firstVector,
                firstVectorOffset_1);
        var first_2 = FloatVector.fromArray(FloatVector.SPECIES_512, firstVector,
                firstVectorOffset_2);
        var first_3 = FloatVector.fromArray(FloatVector.SPECIES_512, firstVector,
                firstVectorOffset_3);
        var first_4 = FloatVector.fromArray(FloatVector.SPECIES_512, firstVector,
                firstVectorOffset_4);

        var second_1 = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                secondVectorOffset_1);
        var second_2 = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                secondVectorOffset_2);
        var second_3 = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                secondVectorOffset_3);
        var second_4 = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                secondVectorOffset_4);

        var diffVector_1 = first_1.sub(second_1);
        var diffVector_2 = first_2.sub(second_2);
        var diffVector_3 = first_3.sub(second_3);
        var diffVector_4 = first_4.sub(second_4);


        var sumVector_1 = diffVector_1.mul(diffVector_1);
        var sumVector_2 = diffVector_2.mul(diffVector_2);
        var sumVector_3 = diffVector_3.mul(diffVector_3);
        var sumVector_4 = diffVector_4.mul(diffVector_4);

        firstVectorOffset_1 = firstVectorOffset_1 + 64;
        firstVectorOffset_2 = firstVectorOffset_2 + 64;
        firstVectorOffset_3 = firstVectorOffset_3 + 64;
        firstVectorOffset_4 = firstVectorOffset_4 + 64;

        secondVectorOffset_1 = secondVectorOffset_1 + 64;
        secondVectorOffset_2 = secondVectorOffset_2 + 64;
        secondVectorOffset_3 = secondVectorOffset_3 + 64;
        secondVectorOffset_4 = secondVectorOffset_4 + 64;

        for (var index = 64; index < size; index += 64,
                firstVectorOffset_1 += 64, firstVectorOffset_2 += 64,
                firstVectorOffset_3 += 64, firstVectorOffset_4 += 64,
                secondVectorOffset_1 += 64, secondVectorOffset_2 += 64,
                secondVectorOffset_3 += 64, secondVectorOffset_4 += 64) {
            first_1 = FloatVector.fromArray(FloatVector.SPECIES_512, firstVector,
                    firstVectorOffset_1);
            first_2 = FloatVector.fromArray(FloatVector.SPECIES_512, firstVector,
                    firstVectorOffset_2);
            first_3 = FloatVector.fromArray(FloatVector.SPECIES_512, firstVector,
                    firstVectorOffset_3);
            first_4 = FloatVector.fromArray(FloatVector.SPECIES_512, firstVector,
                    firstVectorOffset_4);

            second_1 = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                    secondVectorOffset_1);
            second_2 = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                    secondVectorOffset_2);
            second_3 = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                    secondVectorOffset_3);
            second_4 = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                    secondVectorOffset_4);

            diffVector_1 = first_1.sub(second_1);
            diffVector_2 = first_2.sub(second_2);
            diffVector_3 = first_3.sub(second_3);
            diffVector_4 = first_4.sub(second_4);

            sumVector_1 = diffVector_1.fma(diffVector_1, sumVector_1);
            sumVector_2 = diffVector_2.fma(diffVector_2, sumVector_2);
            sumVector_3 = diffVector_3.fma(diffVector_3, sumVector_3);
            sumVector_4 = diffVector_4.fma(diffVector_4, sumVector_4);
        }

        return sumVector_1.add(sumVector_2).add(sumVector_3).add(sumVector_4).reduceLanes(VectorOperators.ADD);
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

    private static float computeL2Distance256Batch(final MemorySegment firstSegment, long firstSegmentOffset,
                                                   final MemorySegment secondSegment, long secondSegmentOffset, int size) {
        assert size == FloatVector.SPECIES_256.loopBound(size);

        var firstSegmentOffset_1 = firstSegmentOffset;
        var firstSegmentOffset_2 = firstSegmentOffset + 8 * Float.BYTES;
        var firstSegmentOffset_3 = firstSegmentOffset + 16 * Float.BYTES;
        var firstSegmentOffset_4 = firstSegmentOffset + 24 * Float.BYTES;

        var secondSegmentOffset_1 = secondSegmentOffset;
        var secondSegmentOffset_2 = secondSegmentOffset + 8 * Float.BYTES;
        var secondSegmentOffset_3 = secondSegmentOffset + 16 * Float.BYTES;
        var secondSegmentOffset_4 = secondSegmentOffset + 24 * Float.BYTES;

        var first_1 = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                firstSegmentOffset_1, ByteOrder.nativeOrder());
        var first_2 = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                firstSegmentOffset_2, ByteOrder.nativeOrder());
        var first_3 = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                firstSegmentOffset_3, ByteOrder.nativeOrder());
        var first_4 = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                firstSegmentOffset_4, ByteOrder.nativeOrder());

        var second_1 = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, secondSegment,
                secondSegmentOffset_1, ByteOrder.nativeOrder());
        var second_2 = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, secondSegment,
                secondSegmentOffset_2, ByteOrder.nativeOrder());
        var second_3 = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, secondSegment,
                secondSegmentOffset_3, ByteOrder.nativeOrder());
        var second_4 = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, secondSegment,
                secondSegmentOffset_4, ByteOrder.nativeOrder());


        var diffVector_1 = first_1.sub(second_1);
        var diffVector_2 = first_2.sub(second_2);
        var diffVector_3 = first_3.sub(second_3);
        var diffVector_4 = first_4.sub(second_4);


        var sumVector_1 = diffVector_1.mul(diffVector_1);
        var sumVector_2 = diffVector_2.mul(diffVector_2);
        var sumVector_3 = diffVector_3.mul(diffVector_3);
        var sumVector_4 = diffVector_4.mul(diffVector_4);


        firstSegmentOffset_1 = firstSegmentOffset_1 + 32 * Float.BYTES;
        firstSegmentOffset_2 = firstSegmentOffset_2 + 32 * Float.BYTES;
        firstSegmentOffset_3 = firstSegmentOffset_3 + 32 * Float.BYTES;
        firstSegmentOffset_4 = firstSegmentOffset_4 + 32 * Float.BYTES;

        secondSegmentOffset_1 = secondSegmentOffset_1 + 32 * Float.BYTES;
        secondSegmentOffset_2 = secondSegmentOffset_2 + 32 * Float.BYTES;
        secondSegmentOffset_3 = secondSegmentOffset_3 + 32 * Float.BYTES;
        secondSegmentOffset_4 = secondSegmentOffset_4 + 32 * Float.BYTES;

        for (var index = 32; index < size; index += 32,
                firstSegmentOffset_1 += 32 * Float.BYTES, firstSegmentOffset_2 += 32 * Float.BYTES,
                firstSegmentOffset_3 += 32 * Float.BYTES, firstSegmentOffset_4 += 32 * Float.BYTES,
                secondSegmentOffset_1 += 32 * Float.BYTES, secondSegmentOffset_2 += 32 * Float.BYTES,
                secondSegmentOffset_3 += 32 * Float.BYTES, secondSegmentOffset_4 += 32 * Float.BYTES) {
            first_1 = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                    firstSegmentOffset_1, ByteOrder.nativeOrder());
            first_2 = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                    firstSegmentOffset_2, ByteOrder.nativeOrder());
            first_3 = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                    firstSegmentOffset_3, ByteOrder.nativeOrder());
            first_4 = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                    firstSegmentOffset_4, ByteOrder.nativeOrder());

            second_1 = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, secondSegment,
                    secondSegmentOffset_1, ByteOrder.nativeOrder());
            second_2 = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, secondSegment,
                    secondSegmentOffset_2, ByteOrder.nativeOrder());
            second_3 = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, secondSegment,
                    secondSegmentOffset_3, ByteOrder.nativeOrder());
            second_4 = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, secondSegment,
                    secondSegmentOffset_4, ByteOrder.nativeOrder());

            diffVector_1 = first_1.sub(second_1);
            diffVector_2 = first_2.sub(second_2);
            diffVector_3 = first_3.sub(second_3);
            diffVector_4 = first_4.sub(second_4);

            sumVector_1 = diffVector_1.fma(diffVector_1, sumVector_1);
            sumVector_2 = diffVector_2.fma(diffVector_2, sumVector_2);
            sumVector_3 = diffVector_3.fma(diffVector_3, sumVector_3);
            sumVector_4 = diffVector_4.fma(diffVector_4, sumVector_4);
        }

        return sumVector_1.add(sumVector_2).add(sumVector_3).add(sumVector_4).reduceLanes(VectorOperators.ADD);
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

    private static float computeL2Distance256Batch(final float[] firstVector, int firstVectorOffset,
                                                   final float[] secondVector, int secondVectorOffset, int size) {
        assert size == FloatVector.SPECIES_256.loopBound(size);

        var firstVectorOffset_1 = firstVectorOffset;
        var firstVectorOffset_2 = firstVectorOffset + 8;
        var firstVectorOffset_3 = firstVectorOffset + 16;
        var firstVectorOffset_4 = firstVectorOffset + 24;

        var secondVectorOffset_1 = secondVectorOffset;
        var secondVectorOffset_2 = secondVectorOffset + 8;
        var secondVectorOffset_3 = secondVectorOffset + 16;
        var secondVectorOffset_4 = secondVectorOffset + 24;

        var first_1 = FloatVector.fromArray(FloatVector.SPECIES_256, firstVector,
                firstVectorOffset_1);
        var first_2 = FloatVector.fromArray(FloatVector.SPECIES_256, firstVector,
                firstVectorOffset_2);
        var first_3 = FloatVector.fromArray(FloatVector.SPECIES_256, firstVector,
                firstVectorOffset_3);
        var first_4 = FloatVector.fromArray(FloatVector.SPECIES_256, firstVector,
                firstVectorOffset_4);

        var second_1 = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                secondVectorOffset_1);
        var second_2 = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                secondVectorOffset_2);
        var second_3 = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                secondVectorOffset_3);
        var second_4 = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                secondVectorOffset_4);


        var diffVector_1 = first_1.sub(second_1);
        var diffVector_2 = first_2.sub(second_2);
        var diffVector_3 = first_3.sub(second_3);
        var diffVector_4 = first_4.sub(second_4);


        var sumVector_1 = diffVector_1.mul(diffVector_1);
        var sumVector_2 = diffVector_2.mul(diffVector_2);
        var sumVector_3 = diffVector_3.mul(diffVector_3);
        var sumVector_4 = diffVector_4.mul(diffVector_4);

        firstVectorOffset_1 = firstVectorOffset_1 + 32;
        firstVectorOffset_2 = firstVectorOffset_2 + 32;
        firstVectorOffset_3 = firstVectorOffset_3 + 32;
        firstVectorOffset_4 = firstVectorOffset_4 + 32;

        secondVectorOffset_1 = secondVectorOffset_1 + 32;
        secondVectorOffset_2 = secondVectorOffset_2 + 32;
        secondVectorOffset_3 = secondVectorOffset_3 + 32;
        secondVectorOffset_4 = secondVectorOffset_4 + 32;

        for (var index = 32; index < size; index += 32,
                firstVectorOffset_1 += 32, firstVectorOffset_2 += 32,
                firstVectorOffset_3 += 32, firstVectorOffset_4 += 32,
                secondVectorOffset_1 += 32, secondVectorOffset_2 += 32,
                secondVectorOffset_3 += 32, secondVectorOffset_4 += 32) {
            first_1 = FloatVector.fromArray(FloatVector.SPECIES_256, firstVector,
                    firstVectorOffset_1);
            first_2 = FloatVector.fromArray(FloatVector.SPECIES_256, firstVector,
                    firstVectorOffset_2);
            first_3 = FloatVector.fromArray(FloatVector.SPECIES_256, firstVector,
                    firstVectorOffset_3);
            first_4 = FloatVector.fromArray(FloatVector.SPECIES_256, firstVector,
                    firstVectorOffset_4);

            second_1 = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                    secondVectorOffset_1);
            second_2 = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                    secondVectorOffset_2);
            second_3 = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                    secondVectorOffset_3);
            second_4 = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                    secondVectorOffset_4);

            diffVector_1 = first_1.sub(second_1);
            diffVector_2 = first_2.sub(second_2);
            diffVector_3 = first_3.sub(second_3);
            diffVector_4 = first_4.sub(second_4);

            sumVector_1 = diffVector_1.fma(diffVector_1, sumVector_1);
            sumVector_2 = diffVector_2.fma(diffVector_2, sumVector_2);
            sumVector_3 = diffVector_3.fma(diffVector_3, sumVector_3);
            sumVector_4 = diffVector_4.fma(diffVector_4, sumVector_4);
        }

        return sumVector_1.add(sumVector_2).add(sumVector_3).add(sumVector_4).reduceLanes(VectorOperators.ADD);
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
