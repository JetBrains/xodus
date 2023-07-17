package jetbrains.exodus.diskann;

import org.junit.Assert;
import org.junit.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;

public class L2DistanceTest {
    @Test
    public void testSmallSegmentZeroOffset() {
        try (var arena = Arena.openConfined()) {
            var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, 2.0f, 3.0f);
            var secondSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, 4.0f, 5.0f);

            for (int i = 16; i >= 1; i /= 2) {
                var distance = L2Distance.computeL2Distance(firstSegment, 0, secondSegment,
                        0,
                        2, i);
                Assert.assertEquals(8.0f, distance, 0.0f);
            }
        }
    }

    @Test
    public void testSmallSegmentFourBatchZeroOffset() {
        try (var arena = Arena.openConfined()) {
            var originalSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, 2.0f, 3.0f);

            var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, 4.0f, 5.0f);
            var secondSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, 5.0f, 6.0f);
            var thirdSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, 6.0f, 7.0f);
            var fourthSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, 8.0f, 8.0f);

            var result = new float[4];

            for (int i = 16; i >= 1; i /= 2) {
                L2Distance.computeL2Distance(originalSegment, 0, firstSegment,
                        0, secondSegment, 0, thirdSegment, 0,
                        fourthSegment, 0, 2, result, i);

                Assert.assertEquals(8.0f, result[0], 0.0f);
                Assert.assertEquals(18.0f, result[1], 0.0f);
                Assert.assertEquals(32.0f, result[2], 0.0f);
                Assert.assertEquals(61.0f, result[3], 0.0f);
            }
        }
    }


    @Test
    public void testBigSegmentZeroOffset() {
        var count = 43;
        try (var arena = Arena.openConfined()) {
            var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count);
            var secondSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count);

            var sum = 0.0f;
            for (var i = 0; i < count; i++) {
                firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, 1.0f * i);
                secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, 3.0f * i);
                sum += 4.0 * i * i;
            }

            for (int i = 16; i >= 1; i /= 2) {
                var distance = L2Distance.computeL2Distance(firstSegment, 0, secondSegment, 0,
                        count, i);
                Assert.assertEquals(sum, distance, 0.0f);
            }
        }
    }

    @Test
    public void testBigSegment4BatchZeroOffset() {
        var count = 43;
        try (var arena = Arena.openConfined()) {
            var originalSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count);

            var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count);
            var secondSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count);
            var thirdSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count);
            var fourthSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count);

            var sum = new float[4];

            for (var i = 0; i < count; i++) {
                originalSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, 1.0f * i);
                firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, 3.0f * i);
                secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, 5.0f * i);
                thirdSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, 7.0f * i);
                fourthSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, 9.0f * i);

                sum[0] += 4.0 * i * i;
                sum[1] += 16.0 * i * i;
                sum[2] += 36.0 * i * i;
                sum[3] += 64.0 * i * i;
            }

            var result = new float[4];
            for (int i = 16; i >= 1; i /= 2) {
                L2Distance.computeL2Distance(originalSegment, 0, firstSegment, 0,
                        secondSegment, 0, thirdSegment, 0, fourthSegment,
                        0, count, result, i);
                Assert.assertArrayEquals(sum, result, 0.0f);
            }
        }
    }

    @Test
    public void testHugeSegmentZeroOffset() {
        for (int k = 1; k <= 3; k++) {
            var count = 107 * k;
            try (var arena = Arena.openConfined()) {
                var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count);
                var secondSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count);

                var sum = 0.0f;
                for (var i = 0; i < count; i++) {
                    firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, 1.0f * i);
                    secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, 3.0f * i);
                    sum += 4.0 * i * i;
                }

                for (int i = 16; i >= 1; i /= 2) {
                    var distance = L2Distance.computeL2Distance(firstSegment, 0, secondSegment, 0,
                            count, i);
                    Assert.assertEquals(sum, distance, 0.0f);
                }
            }
        }

    }

    @Test
    public void testHugeSegment4BatchZeroOffset() {
        var count = 107;
        try (var arena = Arena.openConfined()) {
            var originalSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count);

            var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count);
            var secondSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count);
            var thirdSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count);
            var fourthSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count);

            var sum = new float[4];
            for (var i = 0; i < count; i++) {
                originalSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, 1.0f * i);

                firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, 3.0f * i);
                secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, 4.0f * i);
                thirdSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, 5.0f * i);
                fourthSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, 6.0f * i);

                sum[0] += 4.0 * i * i;
                sum[1] += 9.0 * i * i;
                sum[2] += 16.0 * i * i;
                sum[3] += 25.0 * i * i;
            }

            var result = new float[4];
            for (int i = 16; i >= 1; i /= 2) {
                L2Distance.computeL2Distance(originalSegment, 0, firstSegment, 0,
                        secondSegment, 0, thirdSegment, 0, fourthSegment,
                        0, count, result, i);

                Assert.assertArrayEquals(sum, result, 0.0f);
            }
        }
    }

    @Test
    public void testSmallSegmentNonZeroOffset() {
        try (var arena = Arena.openConfined()) {
            var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, 42.0f, 2.0f, 3.0f);
            var secondSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, 42.0f, 3.0f, 4.0f, 5.0f, 1.0f);

            for (int i = 16; i >= 1; i /= 2) {
                var distance = L2Distance.computeL2Distance(firstSegment, Float.BYTES, secondSegment,
                        2 * Float.BYTES, 2, i);
                Assert.assertEquals(8.0f, distance, 0.0f);
            }
        }
    }

    @Test
    public void testSmallSegmentNonZeroOffset4Batch() {
        try (var arena = Arena.openConfined()) {
            var originSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, 42.0f, 2.0f, 3.0f);

            var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, 42.0f, 3.0f, 4.0f, 5.0f, 1.0f);
            var secondSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, 42.0f, 3.0f, 6.0f, 7.0f, 1.0f);
            var thirdSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, 42.0f, 3.0f, 8.0f, 9.0f, 1.0f);
            var fourthSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, 42.0f, 3.0f, 10.0f, 11.0f, 1.0f);

            var result = new float[4];
            for (int i = 16; i >= 1; i /= 2) {
                L2Distance.computeL2Distance(originSegment,
                        Float.BYTES, firstSegment, 2 * Float.BYTES,
                        secondSegment, 2 * Float.BYTES,
                        thirdSegment, 2 * Float.BYTES,
                        fourthSegment, 2 * Float.BYTES,
                        2, result);

                Assert.assertArrayEquals(new float[]{8.0f, 32.0f, 72.0f, 128.0f}, result, 0.0f);
            }
        }
    }


    @Test
    public void testBigSegmentOffset() {
        var count = 43;
        try (var arena = Arena.openConfined()) {
            var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count + 5);
            var secondSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count + 5);

            firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 0, 42.0f);
            secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 0, 24.0f);

            firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 1, 32.0f);
            secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 1, 23.0f);

            secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 2, 3.0f);

            var sum = 0.0f;
            var firstOffset = 2;
            var secondOffset = 3;
            for (var i = 0; i < count; i++) {
                firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i + firstOffset, 1.0f * i);
                secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i + secondOffset, 3.0f * i);
                sum += 4.0 * i * i;
            }

            for (int i = 16; i >= 1; i /= 2) {
                var distance = L2Distance.computeL2Distance(firstSegment, firstOffset * Float.BYTES,
                        secondSegment, secondOffset * Float.BYTES, count, i);
                Assert.assertEquals(sum, distance, 0.0f);
            }
        }
    }

    @Test
    public void testBigSegmentOffset4Batch() {
        var count = 43;
        try (var arena = Arena.openConfined()) {
            var originSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count + 5);

            var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count + 5);
            var secondSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count + 5);
            var thirdSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count + 5);
            var fourthSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count + 5);

            originSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 0, 42.0f);

            firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 0, 24.0f);
            secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 0, 24.0f);
            thirdSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 0, 24.0f);
            fourthSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 0, 24.0f);

            originSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 1, 32.0f);

            firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 1, 23.0f);
            secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 1, 23.0f);
            thirdSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 1, 23.0f);
            fourthSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 1, 23.0f);

            firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 2, 3.0f);
            secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 2, 3.0f);
            thirdSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 2, 3.0f);
            fourthSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 2, 3.0f);

            var sum = new float[4];

            var origingOffset = 2;
            var otherOffset = 3;
            for (var i = 0; i < count; i++) {
                originSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i + origingOffset, 1.0f * i);

                firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i + otherOffset, 3.0f * i);
                secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i + otherOffset, 4.0f * i);
                thirdSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i + otherOffset, 5.0f * i);
                fourthSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i + otherOffset, 6.0f * i);

                sum[0] += 4.0 * i * i;
                sum[1] += 9.0 * i * i;
                sum[2] += 16.0 * i * i;
                sum[3] += 25.0 * i * i;
            }

            var result = new float[4];
            for (int i = 16; i >= 1; i /= 2) {
                L2Distance.computeL2Distance(originSegment, origingOffset * Float.BYTES,
                        firstSegment, otherOffset * Float.BYTES,
                        secondSegment, otherOffset * Float.BYTES,
                        thirdSegment, otherOffset * Float.BYTES,
                        fourthSegment, otherOffset * Float.BYTES,
                        count, result, i);
                Assert.assertArrayEquals(sum, result, 0.0f);
            }
        }
    }


    @Test
    public void testHugeSegmentOffset() {
        for (int k = 1; k <= 3; k++) {
            var count = 107 * k;
            try (var arena = Arena.openConfined()) {
                var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count + 5);
                var secondSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count + 5);

                firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 0, 42.0f);
                secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 0, 24.0f);

                firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 1, 32.0f);
                secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 1, 23.0f);

                secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 2, 3.0f);

                var sum = 0.0f;
                var firstOffset = 2;
                var secondOffset = 3;
                for (var i = 0; i < count; i++) {
                    firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i + firstOffset, 1.0f * i);
                    secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i + secondOffset, 3.0f * i);
                    sum += 4.0 * i * i;
                }

                for (int i = 16; i >= 1; i /= 2) {
                    var distance = L2Distance.computeL2Distance(firstSegment, firstOffset * Float.BYTES,
                            secondSegment, secondOffset * Float.BYTES, count, i);
                    Assert.assertEquals(sum, distance, 0.0f);
                }
            }
        }
    }

    @Test
    public void testHugeSegmentOffset4Batch() {
        for (int k = 1; k <= 3; k++) {
            var count = 107 * k;
            try (var arena = Arena.openConfined()) {
                var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count + 5);
                var secondSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count + 5);

                firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 0, 42.0f);
                secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 0, 24.0f);

                firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 1, 32.0f);
                secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 1, 23.0f);

                secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 2, 3.0f);

                var sum = 0.0f;
                var firstOffset = 2;
                var secondOffset = 3;
                for (var i = 0; i < count; i++) {
                    firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i + firstOffset, 1.0f * i);
                    secondSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i + secondOffset, 3.0f * i);
                    sum += 4.0 * i * i;
                }

                for (int i = 16; i >= 1; i /= 2) {
                    var distance = L2Distance.computeL2Distance(firstSegment, firstOffset * Float.BYTES,
                            secondSegment, secondOffset * Float.BYTES, count, i);
                    Assert.assertEquals(sum, distance, 0.0f);
                }
            }
        }
    }


    @Test
    public void testSmallSegmentJavaVectorsZeroOffset() {
        try (var arena = Arena.openConfined()) {
            var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, 2.0f, 3.0f);
            var secondVector = new float[]{4.0f, 5.0f};

            for (int i = 16; i >= 1; i /= 2) {
                var distance = L2Distance.computeL2Distance(firstSegment, 0,
                        secondVector, 0, i);
                Assert.assertEquals(8.0f, distance, 0.0f);
            }
        }
    }

    @Test
    public void testBigSegmentJavaVectorsZeroOffset() {
        var count = 43;
        try (var arena = Arena.openConfined()) {
            var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count);
            var secondVector = new float[count];

            var sum = 0.0f;
            for (var i = 0; i < count; i++) {
                firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, 1.0f * i);
                secondVector[i] = 3.0f * i;
                sum += 4.0 * i * i;
            }

            for (int i = 16; i >= 1; i /= 2) {
                var distance = L2Distance.computeL2Distance(firstSegment, 0,
                        secondVector, 0, i);
                Assert.assertEquals(sum, distance, 0.0f);
            }
        }
    }

    @Test
    public void testHugeSegmentJavaVectorsZeroOffset() {
        for (int k = 1; k <= 3; k++) {
            var count = 107 * k;
            try (var arena = Arena.openConfined()) {
                var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count);
                var secondVector = new float[count];

                var sum = 0.0f;
                for (var i = 0; i < count; i++) {
                    firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, 1.0f * i);
                    secondVector[i] = 3.0f * i;
                    sum += 4.0 * i * i;
                }

                for (int i = 16; i >= 1; i /= 2) {
                    var distance = L2Distance.computeL2Distance(firstSegment, 0,
                            secondVector, 0, i);
                    Assert.assertEquals(sum, distance, 0.0f);
                }
            }
        }

    }

    @Test
    public void testSmallSegmentJavaVectorsNonZeroOffset() {
        try (var arena = Arena.openConfined()) {
            var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, 42.0f, 2.0f, 3.0f);
            var secondVector = new float[]{4.0f, 5.0f};

            for (int i = 16; i >= 1; i /= 2) {
                var distance = L2Distance.computeL2Distance(firstSegment, Float.BYTES,
                        secondVector, 0, i);
                Assert.assertEquals(8.0f, distance, 0.0f);
            }
        }
    }

    @Test
    public void testBigSegmentJavaVectorsOffset() {
        var count = 43;
        try (var arena = Arena.openConfined()) {
            var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count + 5);
            var secondVector = new float[count];

            firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 0, 42.0f);
            firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 1, 32.0f);

            var sum = 0.0f;
            var firstOffset = 2;
            for (var i = 0; i < count; i++) {
                firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i + firstOffset, 1.0f * i);
                secondVector[i] = 3.0f * i;
                sum += 4.0 * i * i;
            }


            for (int i = 16; i >= 1; i /= 2) {
                var distance = L2Distance.computeL2Distance(firstSegment, firstOffset * Float.BYTES,
                        secondVector, 0, i);
                Assert.assertEquals(sum, distance, 0.0f);
            }
        }
    }

    @Test
    public void testHugeSegmentJavaVectorsOffset() {
        for (int k = 1; k <= 3; k++) {
            var count = 107 * k;

            try (var arena = Arena.openConfined()) {
                var firstSegment = arena.allocateArray(ValueLayout.JAVA_FLOAT, count + 5);
                var secondVector = new float[count];

                firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 0, 42.0f);
                firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, 1, 32.0f);

                var sum = 0.0f;
                var firstOffset = 2;
                for (var i = 0; i < count; i++) {
                    firstSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i + firstOffset, 1.0f * i);
                    secondVector[i] = 3.0f * i;
                    sum += 4.0 * i * i;
                }


                for (int i = 16; i >= 1; i /= 2) {
                    var distance = L2Distance.computeL2Distance(firstSegment, firstOffset * Float.BYTES,
                            secondVector, 0, i);
                    Assert.assertEquals(sum, distance, 0.0f);
                }
            }
        }

    }

    @Test
    public void testSmallVectorsZeroOffset() {
        var firstVector = new float[]{2.0f, 3.0f};
        var secondVector = new float[]{4.0f, 5.0f};

        for (int i = 16; i >= 1; i /= 2) {
            var distance = L2Distance.computeL2Distance(firstVector,
                    0, secondVector, 0, 2, i);
            Assert.assertEquals(8.0f, distance, 0.0f);
        }
    }

    @Test
    public void testBigVectorsZeroOffset() {
        var count = 43;
        var firstVector = new float[count];
        var secondVector = new float[count];

        var sum = 0.0f;
        for (var i = 0; i < count; i++) {
            firstVector[i] = 1.0f * i;
            secondVector[i] = 3.0f * i;
            sum += 4.0 * i * i;
        }


        for (int i = 16; i >= 1; i /= 2) {
            var distance = L2Distance.computeL2Distance(firstVector, 0,
                    secondVector, 0, count, i);
            Assert.assertEquals(sum, distance, 0.0f);
        }
    }

    @Test
    public void testHugeVectorsZeroOffset() {
        for (int k = 1; k <= 3; k++) {
            var count = 107 * k;
            var firstVector = new float[count];
            var secondVector = new float[count];

            var sum = 0.0f;
            for (var i = 0; i < count; i++) {
                firstVector[i] = 1.0f * i;
                secondVector[i] = 3.0f * i;
                sum += 4.0 * i * i;
            }


            for (int i = 16; i >= 1; i /= 2) {
                var distance = L2Distance.computeL2Distance(firstVector, 0,
                        secondVector, 0, count, i);
                Assert.assertEquals(sum, distance, 0.0f);
            }
        }

    }

    @Test
    public void testSmallVectorsNonZeroOffset() {
        var firstVector = new float[]{42.0f, 2.0f, 3.0f};
        var secondVector = new float[]{42.0f, 3.0f, 4.0f, 5.0f};

        for (int i = 16; i >= 1; i /= 2) {
            var distance = L2Distance.computeL2Distance(firstVector, 1, secondVector,
                    2, 2, i);
            Assert.assertEquals(8.0f, distance, 0.0f);
        }
    }

    @Test
    public void testBigVectorsOffset() {
        var count = 43;

        var firstVector = new float[count + 5];
        var secondVector = new float[count + 5];

        firstVector[0] = 42.0f;
        secondVector[0] = 24.0f;

        firstVector[1] = 32.0f;
        secondVector[1] = 23.0f;

        secondVector[2] = 3.0f;

        var sum = 0.0f;
        var firstOffset = 2;
        var secondOffset = 3;
        for (var i = 0; i < count; i++) {
            firstVector[i + firstOffset] = 1.0f * i;
            secondVector[i + secondOffset] = 3.0f * i;
            sum += 4.0 * i * i;
        }
        for (int i = 16; i >= 1; i /= 2) {
            var distance = L2Distance.computeL2Distance(firstVector, firstOffset, secondVector,
                    secondOffset, count, i);
            Assert.assertEquals(sum, distance, 0.0f);
        }
    }

    @Test
    public void testHugeVectorsOffset() {
        for (int k = 1; k <= 3; k++) {
            var count = 107 * k;

            var firstVector = new float[count + 5];
            var secondVector = new float[count + 5];

            firstVector[0] = 42.0f;
            secondVector[0] = 24.0f;

            firstVector[1] = 32.0f;
            secondVector[1] = 23.0f;

            secondVector[2] = 3.0f;

            var sum = 0.0f;
            var firstOffset = 2;
            var secondOffset = 3;
            for (var i = 0; i < count; i++) {
                firstVector[i + firstOffset] = 1.0f * i;
                secondVector[i + secondOffset] = 3.0f * i;
                sum += 4.0 * i * i;
            }

            for (int i = 16; i >= 1; i /= 2) {
                var distance = L2Distance.computeL2Distance(firstVector, firstOffset, secondVector,
                        secondOffset, count, i);
                Assert.assertEquals(sum, distance, 0.0f);
            }
        }

    }
}
