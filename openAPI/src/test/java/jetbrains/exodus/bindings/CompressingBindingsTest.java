/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
import jetbrains.exodus.util.LightOutputStream;
import org.junit.Assert;
import org.junit.Test;

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
        LongBinding.writeCompressed(output, time);
        Assert.assertTrue(output.size() < 8);
    }

    @Test
    public void testSuccessive() {
        ArrayByteIterable prev = LongBinding.longToCompressedEntry(0);
        for (long i = 1; i < 1000000; ++i) {
            final ArrayByteIterable current = LongBinding.longToCompressedEntry(i);
            Assert.assertTrue(current.compareTo(prev) > 0);
            prev = current;
        }
    }

    @Test
    public void testLongMaxValue() {
        Assert.assertEquals(Long.MAX_VALUE, LongBinding.compressedEntryToLong(LongBinding.longToCompressedEntry(Long.MAX_VALUE)));
        Assert.assertEquals(Long.MAX_VALUE - 1, LongBinding.compressedEntryToLong(LongBinding.longToCompressedEntry(Long.MAX_VALUE - 1)));
    }

    @Test
    public void testSuccessive2() {
        ArrayByteIterable prev = IntegerBinding.intToCompressedEntry(0);
        for (int i = 1; i < 1000000; ++i) {
            final ArrayByteIterable current = IntegerBinding.intToCompressedEntry(i);
            Assert.assertTrue(current.compareTo(prev) > 0);
            prev = current;
        }
    }

    private static void testLong(long l) {
        final LightOutputStream output = new LightOutputStream();
        LongBinding.writeCompressed(output, l);
        final ArrayByteIterable input = output.asArrayByteIterable();
        Assert.assertEquals(l, LongBinding.readCompressed(input.iterator()));
    }

    private static void testInt(int i) {
        final LightOutputStream output = new LightOutputStream();
        IntegerBinding.writeCompressed(output, i);
        final ArrayByteIterable input = output.asArrayByteIterable();
        Assert.assertEquals(i, IntegerBinding.readCompressed(input.iterator()));
    }
}
