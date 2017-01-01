/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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
package jetbrains.exodus.bindings;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.TestFor;
import jetbrains.exodus.util.LightOutputStream;
import org.junit.Assert;
import org.junit.Test;

import static jetbrains.exodus.bindings.IntegerBinding.*;
import static jetbrains.exodus.bindings.IntegerBinding.writeCompressed;
import static jetbrains.exodus.bindings.LongBinding.*;
import static jetbrains.exodus.bindings.LongBinding.writeCompressed;

public class CompressingBindingsTest {

    @Test
    public void testLongs() {
        testLong(0L);
        testLong(1L);
        testLong(9876543210L);
        testLong(19876543210L);
        testLong(119876543210L);
        testLong(1119876543210L);
        testLong(111119876543210L);
        testLong(1111119876543210L);
        testLong(99876543210L);
        testLong(999876543210L);
        testLong(9999876543210L);
        testLong(99999876543210L);
        testLong(999999876543210L);
        testLong(89876543210L);
        testLong(889876543210L);
        testLong(79876543210L);
        testLong(779876543210L);
        testLong(69876543210L);
        testLong(669876543210L);
        testLong(9151314442816847872L);
    }

    @Test
    public void testInts() {
        testInt(0);
        testInt(1);
        testInt(987654321);
        testInt(123456789);
        testInt(1111111111);
        testInt(222222222);
        testInt(333333333);
        testInt(444444444);
        testInt(555555555);
        testInt(666666666);
        testInt(777777777);
        testInt(888888888);
        testInt(999999999);
    }

    @Test
    public void testCurrentTime() {
        final long time = System.currentTimeMillis();
        final LightOutputStream output = new LightOutputStream();
        writeCompressed(output, time);
        Assert.assertTrue(output.size() < 8);
    }

    @Test
    public void testSuccessive() {
        ArrayByteIterable prev = longToCompressedEntry(0);
        for (long i = 1; i < 1000000; ++i) {
            final ArrayByteIterable current = longToCompressedEntry(i);
            Assert.assertTrue(current.compareTo(prev) > 0);
            prev = current;
        }
    }

    @Test
    public void testLongMaxValue() {
        Assert.assertEquals(Long.MAX_VALUE, compressedEntryToLong(longToCompressedEntry(Long.MAX_VALUE)));
        Assert.assertEquals(Long.MAX_VALUE - 1, compressedEntryToLong(longToCompressedEntry(Long.MAX_VALUE - 1)));
    }

    @Test
    public void testSuccessive2() {
        ArrayByteIterable prev = intToCompressedEntry(0);
        for (int i = 1; i < 1000000; ++i) {
            final ArrayByteIterable current = intToCompressedEntry(i);
            Assert.assertTrue(current.compareTo(prev) > 0);
            prev = current;
        }
    }

    @Test
    @TestFor(issues = "XD-537")
    public void testSignedLongs() {
        Assert.assertEquals(1, signedLongToCompressedEntry(-1L).getLength());
        assertSignedLong(-1L);
        Assert.assertEquals(1, signedLongToCompressedEntry(-8L).getLength());
        assertSignedLong(-8L);
        Assert.assertEquals(2, signedLongToCompressedEntry(-9L).getLength());
        assertSignedLong(-9L);
        Assert.assertEquals(2, signedLongToCompressedEntry(-2048L).getLength());
        assertSignedLong(-2048L);
        Assert.assertEquals(3, signedLongToCompressedEntry(-2049L).getLength());
        assertSignedLong(-2049L);
        assertSignedLong((long) (Long.MAX_VALUE * (Math.random() - 0.5)));
    }

    @Test
    @TestFor(issues = "XD-537")
    public void testSignedInts() {
        Assert.assertEquals(1, signedIntToCompressedEntry(-1).getLength());
        assertSignedInt(-1);
        Assert.assertEquals(1, signedIntToCompressedEntry(-16).getLength());
        assertSignedInt(-16);
        Assert.assertEquals(2, signedIntToCompressedEntry(-17).getLength());
        assertSignedInt(-17);
        Assert.assertEquals(2, signedIntToCompressedEntry(-4096).getLength());
        assertSignedInt(-4096);
        Assert.assertEquals(3, signedIntToCompressedEntry(-4097).getLength());
        assertSignedInt(-4097);
        assertSignedInt((int) (Integer.MAX_VALUE * (Math.random() - 0.5)));
    }

    private void assertSignedLong(final long value) {
        Assert.assertEquals(value, compressedEntryToSignedLong(signedLongToCompressedEntry(value)));
    }

    private void assertSignedInt(final int value) {
        Assert.assertEquals(value, compressedEntryToSignedInt(signedIntToCompressedEntry(value)));
    }

    private static void testLong(long l) {
        final LightOutputStream output = new LightOutputStream();
        writeCompressed(output, l);
        final ArrayByteIterable input = output.asArrayByteIterable();
        Assert.assertEquals(l, LongBinding.readCompressed(input.iterator()));
    }

    private static void testInt(int i) {
        final LightOutputStream output = new LightOutputStream();
        writeCompressed(output, i);
        final ArrayByteIterable input = output.asArrayByteIterable();
        Assert.assertEquals(i, IntegerBinding.readCompressed(input.iterator()));
    }
}
