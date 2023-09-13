/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.vectoriadb.index;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.util.Arrays;

public final class DotDistanceFunction implements DistanceFunction {
    public static final DotDistanceFunction INSTANCE = new DotDistanceFunction();
    @Override
    public void computeDistance(MemorySegment originSegment, long originSegmentOffset, MemorySegment firstSegment,
                                long firstSegmentOffset, MemorySegment secondSegment, long secondSegmentOffset,
                                MemorySegment thirdSegment, long thirdSegmentOffset, MemorySegment fourthSegment,
                                long fourthSegmentOffset, int size, float[] result) {
        computeDotDistance(originSegment, originSegmentOffset, firstSegment, firstSegmentOffset, secondSegment,
                secondSegmentOffset, thirdSegment, thirdSegmentOffset, fourthSegment, fourthSegmentOffset, size, result,
                PREFERRED_SPECIES_LENGTH);

    }

    @Override
    public float computeDistance(MemorySegment firstSegment, long firstSegmentFromOffset, float[] secondVector,
                                 int secondVectorOffset, int size) {
        return computeDotDistance(firstSegment, firstSegmentFromOffset, secondVector, secondVectorOffset, size,
                PREFERRED_SPECIES_LENGTH);
    }

    @Override
    public void computeDistance(float[] originVector, int originVectorOffset, MemorySegment firstSegment,
                                long firstSegmentFromOffset, MemorySegment secondSegment, long secondSegmentFromOffset,
                                MemorySegment thirdSegment, long thirdSegmentFromOffset, MemorySegment fourthSegment,
                                long fourthSegmentFromOffset, int size, float[] result) {
        computeDotDistance(originVector, originVectorOffset, firstSegment, firstSegmentFromOffset, secondSegment,
                secondSegmentFromOffset, thirdSegment, thirdSegmentFromOffset, fourthSegment, fourthSegmentFromOffset,
                size, result, PREFERRED_SPECIES_LENGTH);
    }

    @Override
    public float computeDistance(float[] firstVector, int firstVectorFrom, float[] secondVector,
                                 int secondVectorFrom, int size) {
        return computeDotDistance(firstVector, firstVectorFrom, secondVector, secondVectorFrom, size,
                PREFERRED_SPECIES_LENGTH);
    }

    @Override
    public void computeDistance(float[] originVector, int originVectorOffset, float[] firstVector, int firstVectorOffset,
                                float[] secondVector, int secondVectorOffset, float[] thirdVector, int thirdVectorOffset,
                                float[] fourthVector, int fourthVectorOffset, float[] result, int size) {
        computeDotDistance(originVector, originVectorOffset, firstVector, firstVectorOffset, secondVector,
                secondVectorOffset, thirdVector, thirdVectorOffset, fourthVector, fourthVectorOffset, result, size,
                PREFERRED_SPECIES_LENGTH);

    }

    @Override
    public float computeDistance(MemorySegment firstSegment, long firstSegmentOffset,
                                 MemorySegment secondSegment, long secondSegmentOffset, int size) {
        return computeDotDistance(firstSegment, firstSegmentOffset, secondSegment, secondSegmentOffset, size,
                PREFERRED_SPECIES_LENGTH);
    }

    static void computeDotDistance(MemorySegment originSegment, long originSegmentOffset,
                                   MemorySegment firstSegment, long firstSegmentOffset,
                                   MemorySegment secondSegment, long secondSegmentOffset,
                                   MemorySegment thirdSegment, long thirdSegmentOffset,
                                   MemorySegment fourthSegment, long fourthSegmentOffset,
                                   int size, float[] result,
                                   int speciesLength) {
        Arrays.fill(result, 0.0f);

        var step = DistanceFunction.closestSIMDStep(speciesLength, size);

        if (step == 16) {
            var loopBound = FloatVector.SPECIES_512.loopBound(size);

            computeDotDistance512(originSegment, originSegmentOffset, firstSegment,
                    firstSegmentOffset, secondSegment, secondSegmentOffset,
                    thirdSegment, thirdSegmentOffset, fourthSegment, fourthSegmentOffset,
                    result, loopBound);

            size -= loopBound;

            originSegmentOffset += (long) loopBound * Float.BYTES;
            firstSegmentOffset += (long) loopBound * Float.BYTES;
            secondSegmentOffset += (long) loopBound * Float.BYTES;
            thirdSegmentOffset += (long) loopBound * Float.BYTES;
            fourthSegmentOffset += (long) loopBound * Float.BYTES;

            step = DistanceFunction.closestSIMDStep(8, size);
        }
        if (step == 8) {
            var loopBound = FloatVector.SPECIES_256.loopBound(size);
            computeDotDistance256(originSegment, originSegmentOffset, firstSegment,
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
            computeDotDistance128(originSegment, originSegmentOffset, firstSegment,
                    firstSegmentOffset,
                    secondSegment, secondSegmentOffset,
                    thirdSegment, thirdSegmentOffset,
                    fourthSegment, fourthSegmentOffset,
                    result, size);
        }
    }


    static float computeDotDistance(MemorySegment firstSegment, long firstSegmentOffset, MemorySegment secondSegment,
                                    long secondSegmentOffset, int size, int speciesLength) {
        var sum = 0.0f;
        var step = DistanceFunction.closestSIMDStep(speciesLength, size);

        if (step == 16) {
            var loopBound = FloatVector.SPECIES_512.loopBound(size);

            sum += computeDotDistance512(firstSegment, firstSegmentOffset, secondSegment,
                    secondSegmentOffset, loopBound);
            size -= loopBound;
            firstSegmentOffset += (long) loopBound * Float.BYTES;
            secondSegmentOffset += (long) loopBound * Float.BYTES;

            step = DistanceFunction.closestSIMDStep(8, size);
        }
        if (step == 8) {
            var loopBound = FloatVector.SPECIES_256.loopBound(size);
            sum += computeDotDistance256(firstSegment, firstSegmentOffset, secondSegment,
                    secondSegmentOffset, loopBound);
            size -= loopBound;

            firstSegmentOffset += (long) loopBound * Float.BYTES;
            secondSegmentOffset += (long) loopBound * Float.BYTES;
        }

        if (size > 0) {
            sum += computeDotDistance128(firstSegment, firstSegmentOffset, secondSegment,
                    secondSegmentOffset, size);
        }

        return sum;
    }

    static void computeDotDistance(float[] originVector, int originVectorOffset,
                                   MemorySegment firstSegment, long firstSegmentOffset,
                                   MemorySegment secondSegment, long secondSegmentOffset,
                                   MemorySegment thirdSegment, long thirdSegmentOffset,
                                   MemorySegment fourthSegment, long fourthSegmentOffset,
                                   int size,
                                   float[] result,
                                   int speciesLength) {
        Arrays.fill(result, 0.0f);
        var step = DistanceFunction.closestSIMDStep(speciesLength, size);

        if (step == 16) {
            var loopBound = FloatVector.SPECIES_512.loopBound(size);
            computeDotDistance512(originVector, originVectorOffset,
                    firstSegment, firstSegmentOffset,
                    secondSegment, secondSegmentOffset,
                    thirdSegment, thirdSegmentOffset,
                    fourthSegment, fourthSegmentOffset,
                    result,
                    loopBound);

            size -= loopBound;

            originVectorOffset += loopBound;

            firstSegmentOffset += (long) loopBound * Float.BYTES;
            secondSegmentOffset += (long) loopBound * Float.BYTES;
            thirdSegmentOffset += (long) loopBound * Float.BYTES;
            fourthSegmentOffset += (long) loopBound * Float.BYTES;

            step = DistanceFunction.closestSIMDStep(8, size);
        }

        if (step == 8) {
            var loopBound = FloatVector.SPECIES_256.loopBound(size);
            computeDotDistance256(originVector, originVectorOffset,
                    firstSegment, firstSegmentOffset,
                    secondSegment, secondSegmentOffset,
                    thirdSegment, thirdSegmentOffset,
                    fourthSegment, fourthSegmentOffset,
                    result,
                    loopBound);

            size -= loopBound;

            originVectorOffset += loopBound;
            firstSegmentOffset += (long) loopBound * Float.BYTES;
            secondSegmentOffset += (long) loopBound * Float.BYTES;
            thirdSegmentOffset += (long) loopBound * Float.BYTES;
            fourthSegmentOffset += (long) loopBound * Float.BYTES;
        }

        if (size > 0) {
            computeDotDistance128(originVector, originVectorOffset,
                    firstSegment, firstSegmentOffset,
                    secondSegment, secondSegmentOffset,
                    thirdSegment, thirdSegmentOffset,
                    fourthSegment, fourthSegmentOffset,
                    result,
                    size);
        }
    }

    static float computeDotDistance(MemorySegment firstSegment, long firstSegmentOffset, float[] secondVector,
                                    int secondVectorOffset, int size, int speciesLength) {
        var step = DistanceFunction.closestSIMDStep(speciesLength, size);
        var sum = 0.0f;

        if (step == 16) {
            var loopBound = FloatVector.SPECIES_512.loopBound(size);
            sum += computeDotDistance512(firstSegment, firstSegmentOffset, secondVector,
                    secondVectorOffset, loopBound);
            size -= loopBound;
            firstSegmentOffset += (long) loopBound * Float.BYTES;
            secondVectorOffset += loopBound;

            step = DistanceFunction.closestSIMDStep(8, size);
        }
        if (step == 8) {
            var loopBound = FloatVector.SPECIES_256.loopBound(size);
            sum += computeDotDistance256(firstSegment, firstSegmentOffset, secondVector,
                    secondVectorOffset, loopBound);
            size -= loopBound;

            firstSegmentOffset += (long) loopBound * Float.BYTES;
            secondVectorOffset += loopBound;
        }

        if (size > 0) {
            sum += computeDotDistance128(firstSegment, firstSegmentOffset, secondVector,
                    secondVectorOffset, size);
        }

        return sum;
    }

    static void computeDotDistance(float[] originVector, int originVectorOffset,
                                   float[] firstVector, int firstVectorOffset,
                                   float[] secondVector, int secondVectorOffset,
                                   float[] thirdVector, int thirdVectorOffset,
                                   float[] fourthVector, int fourthVectorOffset,
                                   final float[] result,
                                   int size, int speciesLength) {
        Arrays.fill(result, 0.0f);

        var step = DistanceFunction.closestSIMDStep(speciesLength, size);

        if (step == 16) {
            var loopBound = FloatVector.SPECIES_512.loopBound(size);
            computeDotDistance512(originVector, originVectorOffset, firstVector, firstVectorOffset,
                    secondVector, secondVectorOffset,
                    thirdVector, thirdVectorOffset,
                    fourthVector, fourthVectorOffset,
                    result,
                    loopBound);
            size -= loopBound;

            originVectorOffset += loopBound;
            firstVectorOffset += loopBound;
            secondVectorOffset += loopBound;
            thirdVectorOffset += loopBound;
            fourthVectorOffset += loopBound;

            step = DistanceFunction.closestSIMDStep(8, size);
        }

        if (step == 8) {
            var loopBound = FloatVector.SPECIES_256.loopBound(size);
            computeDotDistance256(originVector, originVectorOffset, firstVector, firstVectorOffset,
                    secondVector, secondVectorOffset,
                    thirdVector, thirdVectorOffset,
                    fourthVector, fourthVectorOffset,
                    result,
                    loopBound);
            size -= loopBound;

            originVectorOffset += loopBound;
            firstVectorOffset += loopBound;
            secondVectorOffset += loopBound;
            thirdVectorOffset += loopBound;
            fourthVectorOffset += loopBound;
        }

        if (size > 0) {
            computeDotDistance128(originVector, originVectorOffset, firstVector, firstVectorOffset,
                    secondVector, secondVectorOffset,
                    thirdVector, thirdVectorOffset,
                    fourthVector, fourthVectorOffset,
                    result,
                    size);
        }
    }


    static float computeDotDistance(float[] firstVector, int firstVectorOffset, float[] secondVector,
                                    int secondVectorOffset, int size, int speciesLength) {
        var sum = 0.0f;
        var step = DistanceFunction.closestSIMDStep(speciesLength, size);

        if (step == 16) {
            var loopBound = FloatVector.SPECIES_512.loopBound(size);
            sum += computeDotDistance512(firstVector, firstVectorOffset, secondVector,
                    secondVectorOffset, loopBound);
            size -= loopBound;
            firstVectorOffset += loopBound;
            secondVectorOffset += loopBound;

            step = DistanceFunction.closestSIMDStep(8, size);
        }

        if (step == 8) {
            var loopBound = FloatVector.SPECIES_256.loopBound(size);
            sum += computeDotDistance256(firstVector, firstVectorOffset, secondVector,
                    secondVectorOffset, loopBound);
            size -= loopBound;

            firstVectorOffset += loopBound;
            secondVectorOffset += loopBound;
        }

        if (size > 0) {
            sum += computeDotDistance128(firstVector, firstVectorOffset, secondVector,
                    secondVectorOffset, size);
        }

        return sum;
    }

    private static float computeDotDistance512(final MemorySegment firstSegment, long firstSegmentOffset,
                                               final MemorySegment secondSegment, long secondSegmentOffset, int size) {
        assert size == FloatVector.SPECIES_512.loopBound(size);

        var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                firstSegmentOffset, ByteOrder.nativeOrder());
        var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, secondSegment,
                secondSegmentOffset, ByteOrder.nativeOrder());
        var sumVector = first.mul(second);

        firstSegmentOffset += 16 * Float.BYTES;
        secondSegmentOffset += 16 * Float.BYTES;

        for (var index = 16; index < size; index += 16, firstSegmentOffset += 16 * Float.BYTES,
                secondSegmentOffset += 16 * Float.BYTES) {
            first = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            second = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder());

            sumVector = first.fma(second, sumVector);
        }

        return sumVector.reduceLanes(VectorOperators.ADD);
    }

    private static void computeDotDistance512(final MemorySegment originalSegment, long originalSegmentOffset,
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

        var sumVector_1 = original.mul(first);
        var sumVector_2 = original.mul(second);
        var sumVector_3 = original.mul(third);
        var sumVector_4 = original.mul(fourth);

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

            sumVector_1 = original.fma(first, sumVector_1);
            sumVector_2 = original.fma(second, sumVector_2);
            sumVector_3 = original.fma(third, sumVector_3);
            sumVector_4 = original.fma(fourth, sumVector_4);
        }

        result[0] = sumVector_1.reduceLanes(VectorOperators.ADD);
        result[1] = sumVector_2.reduceLanes(VectorOperators.ADD);
        result[2] = sumVector_3.reduceLanes(VectorOperators.ADD);
        result[3] = sumVector_4.reduceLanes(VectorOperators.ADD);
    }


    private static float computeDotDistance512(final float[] firstVector, int firstVectorOffset,
                                               final float[] secondVector, int secondVectorOffset, int size) {
        var step = 16;
        assert size == FloatVector.SPECIES_512.loopBound(size);

        var first = FloatVector.fromArray(FloatVector.SPECIES_512, firstVector,
                firstVectorOffset);
        var second = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                secondVectorOffset);

        var sumVector = first.mul(second);

        firstVectorOffset += step;
        secondVectorOffset += step;

        for (var index = step; index < size; index += step, firstVectorOffset += step,
                secondVectorOffset += step) {
            first = FloatVector.fromArray(FloatVector.SPECIES_512, firstVector,
                    firstVectorOffset);
            second = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                    secondVectorOffset);

            sumVector = first.fma(second, sumVector);
        }

        return sumVector.reduceLanes(VectorOperators.ADD);
    }

    private static void computeDotDistance512(final float[] originVector, int originVectorOffset,
                                              final float[] firstVector, int firstVectorOffset,
                                              final float[] secondVector, int secondVectorOffset,
                                              final float[] thirdVector, int thirdVectorOffset,
                                              final float[] fourthVector, int fourthVectorOffset,
                                              final float[] result,
                                              final int size) {
        var step = 16;
        assert size == FloatVector.SPECIES_512.loopBound(size);

        var origin = FloatVector.fromArray(FloatVector.SPECIES_512, originVector,
                originVectorOffset);
        var first = FloatVector.fromArray(FloatVector.SPECIES_512, firstVector,
                firstVectorOffset);
        var second = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                firstVectorOffset);
        var third = FloatVector.fromArray(FloatVector.SPECIES_512, thirdVector,
                firstVectorOffset);
        var fourth = FloatVector.fromArray(FloatVector.SPECIES_512, fourthVector,
                firstVectorOffset);

        var sumVector_1 = origin.mul(first);
        var sumVector_2 = origin.mul(second);
        var sumVector_3 = origin.mul(third);
        var sumVector_4 = origin.mul(fourth);

        originVectorOffset += step;
        firstVectorOffset += step;
        secondVectorOffset += step;
        thirdVectorOffset += step;
        fourthVectorOffset += step;

        for (var index = step; index < size; index += step, originVectorOffset += step,
                firstVectorOffset += step, secondVectorOffset += step, thirdVectorOffset += step,
                fourthVectorOffset += step) {
            origin = FloatVector.fromArray(FloatVector.SPECIES_512, originVector,
                    originVectorOffset);
            first = FloatVector.fromArray(FloatVector.SPECIES_512, firstVector,
                    firstVectorOffset);
            second = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                    secondVectorOffset);
            third = FloatVector.fromArray(FloatVector.SPECIES_512, thirdVector,
                    thirdVectorOffset);
            fourth = FloatVector.fromArray(FloatVector.SPECIES_512, fourthVector,
                    fourthVectorOffset);

            sumVector_1 = origin.fma(first, sumVector_1);
            sumVector_2 = origin.fma(second, sumVector_2);
            sumVector_3 = origin.fma(third, sumVector_3);
            sumVector_4 = origin.fma(fourth, sumVector_4);
        }

        result[0] += sumVector_1.reduceLanes(VectorOperators.ADD);
        result[1] += sumVector_2.reduceLanes(VectorOperators.ADD);
        result[2] += sumVector_3.reduceLanes(VectorOperators.ADD);
        result[3] += sumVector_4.reduceLanes(VectorOperators.ADD);
    }

    private static float computeDotDistance512(final MemorySegment firstSegment, long firstSegmentOffset,
                                               final float[] secondVector, int secondVectorOffset, int size) {
        var step = 16;
        var segmentStep = 16 * Float.BYTES;

        var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                firstSegmentOffset, ByteOrder.nativeOrder());
        var second = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                secondVectorOffset);
        var sumVector = first.mul(second);

        firstSegmentOffset += segmentStep;
        secondVectorOffset += step;

        for (var index = step; index < size; index += step, firstSegmentOffset += segmentStep,
                secondVectorOffset += step) {
            first = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            second = FloatVector.fromArray(FloatVector.SPECIES_512, secondVector,
                    secondVectorOffset);
            sumVector = first.fma(second, sumVector);
        }

        return sumVector.reduceLanes(VectorOperators.ADD);
    }

    private static void computeDotDistance512(final float[] originVector, int originVectorOffst,
                                              final MemorySegment firstSegment, long firstSegmentOffset,
                                              final MemorySegment secondSegment, long secondSegmentOffset,
                                              final MemorySegment thirdSegment, long thirdSegmentOffset,
                                              final MemorySegment fourthSegment, long fourthSegmentOffset,
                                              float[] result,
                                              int size) {
        var step = 16;
        var segmentStep = 16 * Float.BYTES;

        var origin = FloatVector.fromArray(FloatVector.SPECIES_512, originVector,
                originVectorOffst);
        var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                firstSegmentOffset, ByteOrder.nativeOrder());
        var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, secondSegment,
                secondSegmentOffset, ByteOrder.nativeOrder());
        var third = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, thirdSegment,
                thirdSegmentOffset, ByteOrder.nativeOrder());
        var fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, fourthSegment,
                fourthSegmentOffset, ByteOrder.nativeOrder());

        var sumVector_1 = origin.mul(first);
        var sumVector_2 = origin.mul(second);
        var sumVector_3 = origin.mul(third);
        var sumVector_4 = origin.mul(fourth);


        originVectorOffst += step;
        firstSegmentOffset += segmentStep;
        secondSegmentOffset += segmentStep;
        thirdSegmentOffset += segmentStep;
        fourthSegmentOffset += segmentStep;

        for (var index = step; index < size; index += step, originVectorOffst += step,
                firstSegmentOffset += segmentStep, secondSegmentOffset += segmentStep, thirdSegmentOffset += segmentStep,
                fourthSegmentOffset += segmentStep) {
            origin = FloatVector.fromArray(FloatVector.SPECIES_512, originVector, originVectorOffst);

            first = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            second = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder());

            third = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, thirdSegment,
                    thirdSegmentOffset, ByteOrder.nativeOrder());

            fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_512, fourthSegment,
                    fourthSegmentOffset, ByteOrder.nativeOrder());

            sumVector_1 = origin.fma(first, sumVector_1);
            sumVector_2 = origin.fma(second, sumVector_2);
            sumVector_3 = origin.fma(third, sumVector_3);
            sumVector_4 = origin.fma(fourth, sumVector_4);
        }

        result[0] += sumVector_1.reduceLanes(VectorOperators.ADD);
        result[1] += sumVector_2.reduceLanes(VectorOperators.ADD);
        result[2] += sumVector_3.reduceLanes(VectorOperators.ADD);
        result[3] += sumVector_4.reduceLanes(VectorOperators.ADD);
    }


    private static float computeDotDistance256(final MemorySegment firstSegment, long firstSegmentOffset,
                                               final MemorySegment secondSegment, long secondSegmentOffset,
                                               int size) {
        var step = 8;
        var segmentStep = 8 * Float.BYTES;

        assert size == FloatVector.SPECIES_256.loopBound(size);

        var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                firstSegmentOffset, ByteOrder.nativeOrder());
        var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, secondSegment,
                secondSegmentOffset, ByteOrder.nativeOrder());
        var sumVector = first.mul(second);

        firstSegmentOffset += segmentStep;
        secondSegmentOffset += segmentStep;

        for (var index = step; index < size; index += step, firstSegmentOffset += segmentStep,
                secondSegmentOffset += segmentStep) {
            first = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            second = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder());
            sumVector = first.fma(second, sumVector);
        }

        return sumVector.reduceLanes(VectorOperators.ADD);
    }

    private static void computeDotDistance256(final MemorySegment originalSegment, long orginalSegmentOffset,
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

        var sumVector_1 = original.mul(first);
        var sumVector_2 = original.mul(second);
        var sumVector_3 = original.mul(third);
        var sumVector_4 = original.mul(fourth);

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

            sumVector_1 = original.fma(first, sumVector_1);
            sumVector_2 = original.fma(second, sumVector_2);
            sumVector_3 = original.fma(third, sumVector_3);
            sumVector_4 = original.fma(fourth, sumVector_4);
        }

        var res_1 = sumVector_1.reduceLanes(VectorOperators.ADD);
        var res_2 = sumVector_2.reduceLanes(VectorOperators.ADD);
        var res_3 = sumVector_3.reduceLanes(VectorOperators.ADD);
        var res_4 = sumVector_4.reduceLanes(VectorOperators.ADD);

        result[0] += res_1;
        result[1] += res_2;
        result[2] += res_3;
        result[3] += res_4;
    }

    private static float computeDotDistance256(final MemorySegment firstSegment, long firstSegmentOffset,
                                               final float[] secondVector, int secondVectorOffset, int size) {
        var step = 8;
        var segmentStep = 8 * Float.BYTES;

        assert size == FloatVector.SPECIES_256.loopBound(size);

        var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                firstSegmentOffset, ByteOrder.nativeOrder());
        var second = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                secondVectorOffset);
        var sumVector = first.mul(second);

        firstSegmentOffset += segmentStep;
        secondVectorOffset += step;

        for (var index = step; index < size; index += step, firstSegmentOffset += segmentStep,
                secondVectorOffset += step) {
            first = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            second = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                    secondVectorOffset);

            sumVector = first.fma(second, sumVector);
        }

        return sumVector.reduceLanes(VectorOperators.ADD);
    }

    private static void computeDotDistance256(final float[] originVector, int originVectorOffset,
                                              final MemorySegment firstSegment, long firstSegmentOffset,
                                              final MemorySegment secondSegment, long secondSegmentOffset,
                                              final MemorySegment thirdSegment, long thirdSegmentOffset,
                                              final MemorySegment fourthSegment, long fourthSegmentOffset,
                                              final float[] result,
                                              int size) {
        var step = 8;
        var segmentStep = 8 * Float.BYTES;

        var origin = FloatVector.fromArray(FloatVector.SPECIES_256, originVector,
                originVectorOffset);

        var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                firstSegmentOffset, ByteOrder.nativeOrder());
        var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, secondSegment,
                secondSegmentOffset, ByteOrder.nativeOrder());
        var third = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, thirdSegment,
                thirdSegmentOffset, ByteOrder.nativeOrder());
        var fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, fourthSegment,
                fourthSegmentOffset, ByteOrder.nativeOrder());

        var sumVector_1 = origin.mul(first);
        var sumVector_2 = origin.mul(second);
        var sumVector_3 = origin.mul(third);
        var sumVector_4 = origin.mul(fourth);

        originVectorOffset += step;
        firstSegmentOffset += segmentStep;
        secondSegmentOffset += segmentStep;
        thirdSegmentOffset += segmentStep;
        fourthSegmentOffset += segmentStep;

        for (var index = step; index < size; index += step, originVectorOffset += step,
                firstSegmentOffset += segmentStep, secondSegmentOffset += segmentStep, thirdSegmentOffset += segmentStep,
                fourthSegmentOffset += segmentStep) {
            origin = FloatVector.fromArray(FloatVector.SPECIES_256, originVector,
                    originVectorOffset);

            first = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            second = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder());
            third = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, thirdSegment,
                    thirdSegmentOffset, ByteOrder.nativeOrder());
            fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_256, fourthSegment,
                    fourthSegmentOffset, ByteOrder.nativeOrder());

            sumVector_1 = origin.fma(first, sumVector_1);
            sumVector_2 = origin.fma(second, sumVector_2);
            sumVector_3 = origin.fma(third, sumVector_3);
            sumVector_4 = origin.fma(fourth, sumVector_4);
        }

        var res_1 = sumVector_1.reduceLanes(VectorOperators.ADD);
        var res_2 = sumVector_2.reduceLanes(VectorOperators.ADD);
        var res_3 = sumVector_3.reduceLanes(VectorOperators.ADD);
        var res_4 = sumVector_4.reduceLanes(VectorOperators.ADD);

        result[0] += res_1;
        result[1] += res_2;
        result[2] += res_3;
        result[3] += res_4;
    }


    private static float computeDotDistance256(final float[] firstVector, int firstVectorOffset,
                                               final float[] secondVector, int secondVectorOffset, int size) {
        var step = 8;
        assert size == FloatVector.SPECIES_256.loopBound(size);

        var first = FloatVector.fromArray(FloatVector.SPECIES_256, firstVector,
                firstVectorOffset);
        var second = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                secondVectorOffset);
        var sumVector = first.mul(second);

        firstVectorOffset += step;
        secondVectorOffset += step;

        for (var index = step; index < size; index += step, firstVectorOffset += step,
                secondVectorOffset += step) {
            first = FloatVector.fromArray(FloatVector.SPECIES_256, firstVector,
                    firstVectorOffset);
            second = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                    secondVectorOffset);

            sumVector = first.fma(second, sumVector);
        }

        return sumVector.reduceLanes(VectorOperators.ADD);
    }

    private static void computeDotDistance256(final float[] originVector, int originVectorOffset,
                                              final float[] firstVector, int firstVectorOffset,
                                              final float[] secondVector, int secondVectorOffset,
                                              final float[] thirdVector, int thirdVectorOffset,
                                              final float[] fourthVector, int fourthVectorOffset,
                                              final float[] result,
                                              int size) {

        var step = 8;
        assert size == FloatVector.SPECIES_256.loopBound(size);

        var origin = FloatVector.fromArray(FloatVector.SPECIES_256, originVector,
                originVectorOffset);
        var first = FloatVector.fromArray(FloatVector.SPECIES_256, firstVector,
                firstVectorOffset);
        var second = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                secondVectorOffset);
        var third = FloatVector.fromArray(FloatVector.SPECIES_256, thirdVector,
                thirdVectorOffset);
        var fourth = FloatVector.fromArray(FloatVector.SPECIES_256, fourthVector,
                fourthVectorOffset);

        var sumVector_1 = origin.mul(first);
        var sumVector_2 = origin.mul(second);
        var sumVector_3 = origin.mul(third);
        var sumVector_4 = origin.mul(fourth);


        originVectorOffset += step;
        firstVectorOffset += step;
        secondVectorOffset += step;
        thirdVectorOffset += step;
        fourthVectorOffset += step;

        for (var index = step; index < size; index += step, originVectorOffset += step,
                firstVectorOffset += step, secondVectorOffset += step, thirdVectorOffset += step,
                fourthVectorOffset += step) {
            origin = FloatVector.fromArray(FloatVector.SPECIES_256, originVector,
                    originVectorOffset);
            first = FloatVector.fromArray(FloatVector.SPECIES_256, firstVector,
                    firstVectorOffset);
            second = FloatVector.fromArray(FloatVector.SPECIES_256, secondVector,
                    secondVectorOffset);
            third = FloatVector.fromArray(FloatVector.SPECIES_256, thirdVector,
                    thirdVectorOffset);

            fourth = FloatVector.fromArray(FloatVector.SPECIES_256, fourthVector,
                    fourthVectorOffset);

            sumVector_1 = origin.fma(first, sumVector_1);
            sumVector_2 = origin.fma(second, sumVector_2);
            sumVector_3 = origin.fma(third, sumVector_3);
            sumVector_4 = origin.fma(fourth, sumVector_4);
        }

        var res_1 = sumVector_1.reduceLanes(VectorOperators.ADD);
        var res_2 = sumVector_2.reduceLanes(VectorOperators.ADD);
        var res_3 = sumVector_3.reduceLanes(VectorOperators.ADD);
        var res_4 = sumVector_4.reduceLanes(VectorOperators.ADD);

        result[0] += res_1;
        result[1] += res_2;
        result[2] += res_3;
        result[3] += res_4;
    }

    private static float computeDotDistance128(final MemorySegment firstSegment, long firstSegmentOffset,
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
            var sumVector = first.mul(second);

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

                sumVector = first.fma(second, sumVector);
            }

            if (index < size) {
                var mask = FloatVector.SPECIES_128.indexInRange(index, size);

                first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                        firstSegmentOffset, ByteOrder.nativeOrder(), mask);
                second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment,
                        secondSegmentOffset, ByteOrder.nativeOrder(), mask);

                sumVector = first.mul(second, mask).add(sumVector);
            }

            sum = sumVector.reduceLanes(VectorOperators.ADD);
        } else {
            var mask = FloatVector.SPECIES_128.indexInRange(index, size);

            var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder(), mask);
            var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder(), mask);
            var sumVector = first.mul(second, mask);

            sum = sumVector.reduceLanes(VectorOperators.ADD, mask);
        }

        return sum;
    }

    private static void computeDotDistance128(final MemorySegment originalSegment, long originalSegmentOffset,
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

            var origin = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, originalSegment,
                    originalSegmentOffset, ByteOrder.nativeOrder());
            var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder());
            var third = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, thirdSegment,
                    thirdSegmentOffset, ByteOrder.nativeOrder());
            var fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, fourthSegment,
                    fourthSegmentOffset, ByteOrder.nativeOrder());

            var sumVector_1 = origin.mul(first);
            var sumVector_2 = origin.mul(second);
            var sumVector_3 = origin.mul(third);
            var sumVector_4 = origin.mul(fourth);

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

                origin = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, originalSegment,
                        originalSegmentOffset, ByteOrder.nativeOrder());
                first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                        firstSegmentOffset, ByteOrder.nativeOrder());
                second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment,
                        secondSegmentOffset, ByteOrder.nativeOrder());
                third = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, thirdSegment,
                        thirdSegmentOffset, ByteOrder.nativeOrder());
                fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, fourthSegment,
                        fourthSegmentOffset, ByteOrder.nativeOrder());

                sumVector_1 = origin.fma(first, sumVector_1);
                sumVector_2 = origin.fma(second, sumVector_2);
                sumVector_3 = origin.fma(third, sumVector_3);
                sumVector_4 = origin.fma(fourth, sumVector_4);
            }

            if (index < size) {
                var mask = FloatVector.SPECIES_128.indexInRange(index, size);

                origin = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, originalSegment,
                        originalSegmentOffset, ByteOrder.nativeOrder(), mask);
                first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                        firstSegmentOffset, ByteOrder.nativeOrder(), mask);
                second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment, secondSegmentOffset,
                        ByteOrder.nativeOrder(), mask);
                third = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, thirdSegment, thirdSegmentOffset,
                        ByteOrder.nativeOrder(), mask);
                fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, fourthSegment, fourthSegmentOffset,
                        ByteOrder.nativeOrder(), mask);

                sumVector_1 = origin.mul(first, mask).add(sumVector_1);
                sumVector_2 = origin.mul(second, mask).add(sumVector_2);
                sumVector_3 = origin.mul(third, mask).add(sumVector_3);
                sumVector_4 = origin.mul(fourth, mask).add(sumVector_4);
            }

            var res_1 = sumVector_1.reduceLanes(VectorOperators.ADD);
            var res_2 = sumVector_2.reduceLanes(VectorOperators.ADD);
            var res_3 = sumVector_3.reduceLanes(VectorOperators.ADD);
            var res_4 = sumVector_4.reduceLanes(VectorOperators.ADD);

            result[0] += res_1;
            result[1] += res_2;
            result[2] += res_3;
            result[3] += res_4;
        } else {
            var mask = FloatVector.SPECIES_128.indexInRange(index, size);

            var origin = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, originalSegment,
                    originalSegmentOffset, ByteOrder.nativeOrder(), mask);
            var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder(), mask);
            var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder(), mask);
            var third = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, thirdSegment, thirdSegmentOffset,
                    ByteOrder.nativeOrder(), mask);
            var fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, fourthSegment, fourthSegmentOffset,
                    ByteOrder.nativeOrder(), mask);

            var sumVector_1 = origin.mul(first, mask);
            var sumVector_2 = origin.mul(second, mask);
            var sumVector_3 = origin.mul(third, mask);
            var sumVector_4 = origin.mul(fourth, mask);

            var res_1 = sumVector_1.reduceLanes(VectorOperators.ADD, mask);
            var res_2 = sumVector_2.reduceLanes(VectorOperators.ADD, mask);
            var res_3 = sumVector_3.reduceLanes(VectorOperators.ADD, mask);
            var res_4 = sumVector_4.reduceLanes(VectorOperators.ADD, mask);

            result[0] += res_1;
            result[1] += res_2;
            result[2] += res_3;
            result[3] += res_4;
        }
    }


    private static float computeDotDistance128(final MemorySegment firstSegment, long firstSegmentOffset,
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
            var sumVector = first.mul(second);

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

                sumVector = first.fma(second, sumVector);
            }

            if (index < size) {
                var mask = FloatVector.SPECIES_128.indexInRange(index, size);

                first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                        firstSegmentOffset, ByteOrder.nativeOrder(), mask);
                second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                        secondVectorOffset, mask);

                sumVector = first.mul(second, mask).add(sumVector);
            }

            sum = sumVector.reduceLanes(VectorOperators.ADD);
        } else {
            var mask = FloatVector.SPECIES_128.indexInRange(index, size);

            var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder(), mask);
            var second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                    secondVectorOffset, mask);

            var sumVector = first.mul(second, mask);

            sum = sumVector.reduceLanes(VectorOperators.ADD, mask);
        }

        return sum;
    }

    private static void computeDotDistance128(final float[] originVector, int originVectorOffset,
                                              final MemorySegment firstSegment, long firstSegmentOffset,
                                              final MemorySegment secondSegment, long secondSegmentOffset,
                                              final MemorySegment thirdSegment, long thirdSegmentOffset,
                                              final MemorySegment fourthSegment, long fourthSegmentOffset,
                                              final float[] result,
                                              int size) {
        var step = 4;
        var index = 0;

        if (size >= step) {
            var segmentStep = 4 * Float.BYTES;

            var origin = FloatVector.fromArray(FloatVector.SPECIES_128, originVector,
                    originVectorOffset);
            var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder());
            var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder());
            var third = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, thirdSegment,
                    thirdSegmentOffset, ByteOrder.nativeOrder());
            var fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, fourthSegment,
                    fourthSegmentOffset, ByteOrder.nativeOrder());

            var sumVector_1 = origin.mul(first);
            var sumVector_2 = origin.mul(second);
            var sumVector_3 = origin.mul(third);
            var sumVector_4 = origin.mul(fourth);


            originVectorOffset += step;

            firstSegmentOffset += segmentStep;
            secondSegmentOffset += segmentStep;
            thirdSegmentOffset += segmentStep;
            fourthSegmentOffset += segmentStep;

            index = step;

            var loopBound = FloatVector.SPECIES_128.loopBound(size);

            for (; index < loopBound; index += step, originVectorOffset += step,
                    firstSegmentOffset += segmentStep, secondSegmentOffset += segmentStep, thirdSegmentOffset += segmentStep,
                    fourthSegmentOffset += segmentStep) {
                origin = FloatVector.fromArray(FloatVector.SPECIES_128, originVector,
                        originVectorOffset);
                first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                        firstSegmentOffset, ByteOrder.nativeOrder());
                second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment,
                        secondSegmentOffset, ByteOrder.nativeOrder());
                third = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, thirdSegment,
                        thirdSegmentOffset, ByteOrder.nativeOrder());
                fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, fourthSegment,
                        fourthSegmentOffset, ByteOrder.nativeOrder());


                sumVector_1 = origin.fma(first, sumVector_1);
                sumVector_2 = origin.fma(second, sumVector_2);
                sumVector_3 = origin.fma(third, sumVector_3);
                sumVector_4 = origin.fma(fourth, sumVector_4);
            }

            if (index < size) {
                var mask = FloatVector.SPECIES_128.indexInRange(index, size);

                origin = FloatVector.fromArray(FloatVector.SPECIES_128, originVector,
                        originVectorOffset, mask);
                first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                        firstSegmentOffset, ByteOrder.nativeOrder(), mask);
                second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment,
                        secondSegmentOffset, ByteOrder.nativeOrder(), mask);
                third = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, thirdSegment,
                        thirdSegmentOffset, ByteOrder.nativeOrder(), mask);
                fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, fourthSegment,
                        fourthSegmentOffset, ByteOrder.nativeOrder(), mask);

                var mulVector_1 = origin.mul(first, mask);
                var mulVector_2 = origin.mul(second, mask);
                var mulVector_3 = origin.mul(third, mask);
                var mulVector_4 = origin.mul(fourth, mask);

                sumVector_1 = mulVector_1.add(sumVector_1);
                sumVector_2 = mulVector_2.add(sumVector_2);
                sumVector_3 = mulVector_3.add(sumVector_3);
                sumVector_4 = mulVector_4.add(sumVector_4);
            }

            var res_1 = sumVector_1.reduceLanes(VectorOperators.ADD);
            var res_2 = sumVector_2.reduceLanes(VectorOperators.ADD);
            var res_3 = sumVector_3.reduceLanes(VectorOperators.ADD);
            var res_4 = sumVector_4.reduceLanes(VectorOperators.ADD);


            result[0] += res_1;
            result[1] += res_2;
            result[2] += res_3;
            result[3] += res_4;
        } else {
            var mask = FloatVector.SPECIES_128.indexInRange(index, size);

            var origin = FloatVector.fromArray(FloatVector.SPECIES_128, originVector,
                    originVectorOffset, mask);
            var first = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, firstSegment,
                    firstSegmentOffset, ByteOrder.nativeOrder(), mask);
            var second = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, secondSegment,
                    secondSegmentOffset, ByteOrder.nativeOrder(), mask);
            var third = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, thirdSegment,
                    thirdSegmentOffset, ByteOrder.nativeOrder(), mask);
            var fourth = FloatVector.fromMemorySegment(FloatVector.SPECIES_128, fourthSegment,
                    fourthSegmentOffset, ByteOrder.nativeOrder(), mask);

            var sumVector_1 = origin.mul(first, mask);
            var sumVector_2 = origin.mul(second, mask);
            var sumVector_3 = origin.mul(third, mask);
            var sumVector_4 = origin.mul(fourth, mask);

            var res_1 = sumVector_1.reduceLanes(VectorOperators.ADD, mask);
            var res_2 = sumVector_2.reduceLanes(VectorOperators.ADD, mask);
            var res_3 = sumVector_3.reduceLanes(VectorOperators.ADD, mask);
            var res_4 = sumVector_4.reduceLanes(VectorOperators.ADD, mask);

            result[0] += res_1;
            result[1] += res_2;
            result[2] += res_3;
            result[3] += res_4;
        }
    }

    private static float computeDotDistance128(final float[] firstVector, int firstVectorOffset,
                                               final float[] secondVector, int secondVectorOffset, int size) {
        var step = 4;
        var index = 0;
        var sum = 0.0f;

        if (size >= step) {
            var first = FloatVector.fromArray(FloatVector.SPECIES_128, firstVector,
                    firstVectorOffset);
            var second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                    secondVectorOffset);
            var sumVector = first.mul(second);

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

                sumVector = first.fma(second, sumVector);
            }

            if (index < size) {
                var mask = FloatVector.SPECIES_128.indexInRange(index, size);

                first = FloatVector.fromArray(FloatVector.SPECIES_128, firstVector,
                        firstVectorOffset, mask);
                second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                        secondVectorOffset, mask);

                sumVector = first.mul(second, mask).add(sumVector);
            }

            sum = sumVector.reduceLanes(VectorOperators.ADD);
        } else {
            var mask = FloatVector.SPECIES_128.indexInRange(index, size);

            var first = FloatVector.fromArray(FloatVector.SPECIES_128, firstVector,
                    firstVectorOffset, mask);
            var second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                    secondVectorOffset, mask);

            var sumVector = first.mul(second, mask);

            sum = sumVector.reduceLanes(VectorOperators.ADD, mask);
        }

        return sum;
    }

    private static void computeDotDistance128(final float[] originVector, int originVectorOffset,
                                              final float[] firstVector, int firstVectorOffset,
                                              final float[] secondVector, int secondVectorOffset,
                                              final float[] thirdVector, int thirdVectorOffset,
                                              final float[] fourthVector, int fourthVectorOffset,
                                              final float[] result,
                                              int size) {
        var step = 4;
        var index = 0;

        if (size >= step) {
            var origin = FloatVector.fromArray(FloatVector.SPECIES_128, originVector,
                    originVectorOffset);
            var first = FloatVector.fromArray(FloatVector.SPECIES_128, firstVector,
                    firstVectorOffset);
            var second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                    secondVectorOffset);
            var third = FloatVector.fromArray(FloatVector.SPECIES_128, thirdVector,
                    thirdVectorOffset);
            var fourth = FloatVector.fromArray(FloatVector.SPECIES_128, fourthVector,
                    fourthVectorOffset);

            var sumVector_1 = origin.mul(first);
            var sumVector_2 = origin.mul(second);
            var sumVector_3 = origin.mul(third);
            var sumVector_4 = origin.mul(fourth);

            index = step;
            originVectorOffset += step;
            firstVectorOffset += step;
            secondVectorOffset += step;
            thirdVectorOffset += step;
            fourthVectorOffset += step;

            var loopBound = FloatVector.SPECIES_128.loopBound(size);

            for (; index < loopBound; index += step, originVectorOffset += step,
                    firstVectorOffset += step, secondVectorOffset += step, thirdVectorOffset += step,
                    fourthVectorOffset += step) {
                origin = FloatVector.fromArray(FloatVector.SPECIES_128, originVector,
                        originVectorOffset);
                first = FloatVector.fromArray(FloatVector.SPECIES_128, firstVector,
                        firstVectorOffset);
                second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                        secondVectorOffset);
                third = FloatVector.fromArray(FloatVector.SPECIES_128, thirdVector,
                        thirdVectorOffset);
                fourth = FloatVector.fromArray(FloatVector.SPECIES_128, fourthVector,
                        fourthVectorOffset);


                sumVector_1 = origin.fma(first, sumVector_1);
                sumVector_2 = origin.fma(second, sumVector_2);
                sumVector_3 = origin.fma(third, sumVector_3);
                sumVector_4 = origin.fma(fourth, sumVector_4);
            }

            if (index < size) {
                var mask = FloatVector.SPECIES_128.indexInRange(index, size);

                origin = FloatVector.fromArray(FloatVector.SPECIES_128, originVector,
                        originVectorOffset, mask);
                first = FloatVector.fromArray(FloatVector.SPECIES_128, firstVector,
                        firstVectorOffset, mask);
                second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                        secondVectorOffset, mask);
                third = FloatVector.fromArray(FloatVector.SPECIES_128, thirdVector,
                        thirdVectorOffset, mask);
                fourth = FloatVector.fromArray(FloatVector.SPECIES_128, fourthVector,
                        fourthVectorOffset, mask);

                var mul_1 = origin.mul(first, mask);
                var mul_2 = origin.mul(second, mask);
                var mul_3 = origin.mul(third, mask);
                var mul_4 = origin.mul(fourth, mask);

                sumVector_1 = mul_1.add(sumVector_1);
                sumVector_2 = mul_2.add(sumVector_2);
                sumVector_3 = mul_3.add(sumVector_3);
                sumVector_4 = mul_4.add(sumVector_4);
            }


            var res_1 = sumVector_1.reduceLanes(VectorOperators.ADD);
            var res_2 = sumVector_2.reduceLanes(VectorOperators.ADD);
            var res_3 = sumVector_3.reduceLanes(VectorOperators.ADD);
            var res_4 = sumVector_4.reduceLanes(VectorOperators.ADD);

            result[0] += res_1;
            result[1] += res_2;
            result[2] += res_3;
            result[3] += res_4;
        } else {
            var mask = FloatVector.SPECIES_128.indexInRange(index, size);

            var origin = FloatVector.fromArray(FloatVector.SPECIES_128, originVector,
                    originVectorOffset, mask);
            var first = FloatVector.fromArray(FloatVector.SPECIES_128, firstVector,
                    firstVectorOffset, mask);
            var second = FloatVector.fromArray(FloatVector.SPECIES_128, secondVector,
                    secondVectorOffset, mask);
            var third = FloatVector.fromArray(FloatVector.SPECIES_128, thirdVector,
                    thirdVectorOffset, mask);
            var fourth = FloatVector.fromArray(FloatVector.SPECIES_128, fourthVector,
                    fourthVectorOffset, mask);

            var sumVector_1 = origin.mul(first, mask);
            var sumVector_2 = origin.mul(second, mask);
            var sumVector_3 = origin.mul(third, mask);
            var sumVector_4 = origin.mul(fourth, mask);

            var res_1 = sumVector_1.reduceLanes(VectorOperators.ADD, mask);
            var res_2 = sumVector_2.reduceLanes(VectorOperators.ADD, mask);
            var res_3 = sumVector_3.reduceLanes(VectorOperators.ADD, mask);
            var res_4 = sumVector_4.reduceLanes(VectorOperators.ADD, mask);

            result[0] += res_1;
            result[1] += res_2;
            result[2] += res_3;
            result[3] += res_4;
        }
    }
}
