/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.bindings.CompressedUnsignedLongArrayByteIterable;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.LongBinding;
import org.junit.Assert;
import org.junit.Test;

public class PrimitiveIterableTests {

    @Test
    public void testIntByteIterable() {
        assertMatchesArray(IntegerBinding.intToEntry(Integer.MIN_VALUE), new byte[]{0, 0, 0, 0});
        assertMatchesArray(IntegerBinding.intToEntry(Integer.MIN_VALUE + 1), new byte[]{0, 0, 0, 1});
        assertMatchesArray(IntegerBinding.intToEntry(-2), new byte[]{127, -1, -1, -2});
        assertMatchesArray(IntegerBinding.intToEntry(-1), new byte[]{127, -1, -1, -1});
        assertMatchesArray(IntegerBinding.intToEntry(0), new byte[]{-128, 0, 0, 0});
        assertMatchesArray(IntegerBinding.intToEntry(0x01020304), new byte[]{-127, 2, 3, 4});
        assertMatchesArray(IntegerBinding.intToEntry(Integer.MAX_VALUE), new byte[]{-1, -1, -1, -1});
    }

    @Test
    public void testLongByteIterable() {
        assertMatchesArray(LongBinding.longToEntry(Long.MIN_VALUE), new byte[]{0, 0, 0, 0, 0, 0, 0, 0});
        assertMatchesArray(LongBinding.longToEntry(Long.MIN_VALUE + 1), new byte[]{0, 0, 0, 0, 0, 0, 0, 1});
        assertMatchesArray(LongBinding.longToEntry(-2), new byte[]{127, -1, -1, -1, -1, -1, -1, -2});
        assertMatchesArray(LongBinding.longToEntry(-1), new byte[]{127, -1, -1, -1, -1, -1, -1, -1});
        assertMatchesArray(LongBinding.longToEntry(0), new byte[]{-128, 0, 0, 0, 0, 0, 0, 0});
        assertMatchesArray(LongBinding.longToEntry(0x0102030405060708L), new byte[]{-127, 2, 3, 4, 5, 6, 7, 8});
        assertMatchesArray(LongBinding.longToEntry(Long.MAX_VALUE), new byte[]{-1, -1, -1, -1, -1, -1, -1, -1});
    }

    @Test
    public void testCompressedUnsignedLongArrayByteIterable() {
        testCompressedUnsignedLongArrayByteIterable(new long[0]);
        testCompressedUnsignedLongArrayByteIterable(new long[]{0});
        testCompressedUnsignedLongArrayByteIterable(new long[]{0xff, 0xffff, 0x10000, 0, 0xffff, 0xff, 2L << 24, 0});
        testCompressedUnsignedLongArrayByteIterable(new long[]{0xff, 0xffff, 0x10000, 0, 0xffff, 0xff, 2L << 24, 0,
                31415925853L, 2718281828L, 0, 2L << 56, 0, 2L << 48});
        assertMatchesArray(
                CompressedUnsignedLongArrayByteIterable.getIterable(new long[]{315019182, 2L << 32}),
                new byte[]{5, 0, 18, -58, -49, -82, 2, 0, 0, 0, 0}
        );
    }

    @Test
    public void testCompressedUnsignedLongArrayByteIterableIllegalArgumentException() {
        try {
            CompressedUnsignedLongArrayByteIterable.getIterable(new long[]{-1});
        } catch (IllegalArgumentException e) {
            return;
        }
        Assert.assertTrue(false);
    }

    @Test
    public void testInts() {
        testInt(Integer.MIN_VALUE);
        testInt(Integer.MIN_VALUE + 1);
        testInt(-2);
        testInt(-1);
        testInt(0);
        testInt(0x01020304);
        testInt(Integer.MAX_VALUE);
    }

    @Test
    public void testLongs() {
        testLong(Long.MIN_VALUE);
        testLong(Long.MIN_VALUE + 1);
        testLong(-2L);
        testLong(-1L);
        testLong(0L);
        testLong(0x0102030405060708L);
        testLong(Integer.MAX_VALUE);
    }

    private void testInt(int i) {
        Assert.assertEquals(i, IntegerBinding.entryToInt(IntegerBinding.intToEntry(i)));
    }

    private void testLong(long i) {
        Assert.assertEquals(i, LongBinding.entryToLong(LongBinding.longToEntry(i)));
    }

    private void assertMatchesArray(ByteIterable iterable, byte[] array) {
        final ByteIterator iterator = iterable.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            Assert.assertNotSame("Iterator length", array.length, i);
            byte result = iterator.next();
            Assert.assertEquals("Byte [" + i + ']', array[i], result);
            ++i;
        }
        Assert.assertEquals("Iterator length", array.length, i);
    }

    private void testCompressedUnsignedLongArrayByteIterable(long[] longArray) {
        final long[] longs = new long[longArray.length];
        final ByteIterable it = CompressedUnsignedLongArrayByteIterable.getIterable(longArray);
        CompressedUnsignedLongArrayByteIterable.loadLongs(longs, it.iterator());
        Assert.assertEquals(longArray.length, longs.length);
        for (int i = 0; i < longArray.length; i++) {
            long l = longArray[i];
            Assert.assertEquals(l, longs[i]);
        }
    }
}
