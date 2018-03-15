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
package jetbrains.exodus.entitystore

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.CompressedUnsignedLongArrayByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import org.junit.Assert
import org.junit.Test

class PrimitiveIterableTests {

    @Test
    fun testIntByteIterable() {
        assertMatchesArray(IntegerBinding.intToEntry(Integer.MIN_VALUE), byteArrayOf(0, 0, 0, 0))
        assertMatchesArray(IntegerBinding.intToEntry(Integer.MIN_VALUE + 1), byteArrayOf(0, 0, 0, 1))
        assertMatchesArray(IntegerBinding.intToEntry(-2), byteArrayOf(127, -1, -1, -2))
        assertMatchesArray(IntegerBinding.intToEntry(-1), byteArrayOf(127, -1, -1, -1))
        assertMatchesArray(IntegerBinding.intToEntry(0), byteArrayOf(-128, 0, 0, 0))
        assertMatchesArray(IntegerBinding.intToEntry(0x01020304), byteArrayOf(-127, 2, 3, 4))
        assertMatchesArray(IntegerBinding.intToEntry(Integer.MAX_VALUE), byteArrayOf(-1, -1, -1, -1))
    }

    @Test
    fun testLongByteIterable() {
        assertMatchesArray(LongBinding.longToEntry(java.lang.Long.MIN_VALUE), byteArrayOf(0, 0, 0, 0, 0, 0, 0, 0))
        assertMatchesArray(LongBinding.longToEntry(java.lang.Long.MIN_VALUE + 1), byteArrayOf(0, 0, 0, 0, 0, 0, 0, 1))
        assertMatchesArray(LongBinding.longToEntry(-2), byteArrayOf(127, -1, -1, -1, -1, -1, -1, -2))
        assertMatchesArray(LongBinding.longToEntry(-1), byteArrayOf(127, -1, -1, -1, -1, -1, -1, -1))
        assertMatchesArray(LongBinding.longToEntry(0), byteArrayOf(-128, 0, 0, 0, 0, 0, 0, 0))
        assertMatchesArray(LongBinding.longToEntry(0x0102030405060708L), byteArrayOf(-127, 2, 3, 4, 5, 6, 7, 8))
        assertMatchesArray(LongBinding.longToEntry(java.lang.Long.MAX_VALUE), byteArrayOf(-1, -1, -1, -1, -1, -1, -1, -1))
    }

    @Test
    fun testCompressedUnsignedLongArrayByteIterable() {
        testCompressedUnsignedLongArrayByteIterable(LongArray(0))
        testCompressedUnsignedLongArrayByteIterable(longArrayOf(0))
        testCompressedUnsignedLongArrayByteIterable(longArrayOf(0xff, 0xffff, 0x10000, 0, 0xffff, 0xff, 2L shl 24, 0))
        testCompressedUnsignedLongArrayByteIterable(longArrayOf(0xff, 0xffff, 0x10000, 0, 0xffff, 0xff, 2L shl 24, 0, 31415925853L, 2718281828L, 0, 2L shl 56, 0, 2L shl 48))
        assertMatchesArray(
                CompressedUnsignedLongArrayByteIterable.getIterable(longArrayOf(315019182, 2L shl 32)),
                byteArrayOf(5, 0, 18, -58, -49, -82, 2, 0, 0, 0, 0)
        )
    }

    @Test
    fun testCompressedUnsignedLongArrayByteIterableIllegalArgumentException() {
        try {
            CompressedUnsignedLongArrayByteIterable.getIterable(longArrayOf(-1))
        } catch (e: IllegalArgumentException) {
            return
        }

        Assert.assertTrue(false)
    }

    @Test
    fun testInts() {
        testInt(Integer.MIN_VALUE)
        testInt(Integer.MIN_VALUE + 1)
        testInt(-2)
        testInt(-1)
        testInt(0)
        testInt(0x01020304)
        testInt(Integer.MAX_VALUE)
    }

    @Test
    fun testLongs() {
        testLong(java.lang.Long.MIN_VALUE)
        testLong(java.lang.Long.MIN_VALUE + 1)
        testLong(-2L)
        testLong(-1L)
        testLong(0L)
        testLong(0x0102030405060708L)
        testLong(Integer.MAX_VALUE.toLong())
    }

    private fun testInt(i: Int) {
        Assert.assertEquals(i.toLong(), IntegerBinding.entryToInt(IntegerBinding.intToEntry(i)).toLong())
    }

    private fun testLong(i: Long) {
        Assert.assertEquals(i, LongBinding.entryToLong(LongBinding.longToEntry(i)))
    }

    private fun assertMatchesArray(iterable: ByteIterable, array: ByteArray) {
        val iterator = iterable.iterator()
        var i = 0
        while (iterator.hasNext()) {
            Assert.assertNotSame("Iterator length", array.size, i)
            val result = iterator.next()
            Assert.assertEquals("Byte [" + i + ']'.toString(), array[i].toLong(), result.toLong())
            ++i
        }
        Assert.assertEquals("Iterator length", array.size.toLong(), i.toLong())
    }

    private fun testCompressedUnsignedLongArrayByteIterable(longArray: LongArray) {
        val longs = LongArray(longArray.size)
        val it = CompressedUnsignedLongArrayByteIterable.getIterable(longArray)
        CompressedUnsignedLongArrayByteIterable.loadLongs(longs, it.iterator())
        Assert.assertEquals(longArray.size.toLong(), longs.size.toLong())
        for (i in longArray.indices) {
            val l = longArray[i]
            Assert.assertEquals(l, longs[i])
        }
    }
}
