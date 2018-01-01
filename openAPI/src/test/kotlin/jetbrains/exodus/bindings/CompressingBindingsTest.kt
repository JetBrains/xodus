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
package jetbrains.exodus.bindings

import jetbrains.exodus.TestFor
import jetbrains.exodus.bindings.IntegerBinding.*
import jetbrains.exodus.bindings.LongBinding.*
import jetbrains.exodus.util.LightOutputStream
import org.junit.Assert
import org.junit.Test

class CompressingBindingsTest {

    @Test
    fun testLongs() {
        testLong(0L)
        testLong(1L)
        testLong(9876543210L)
        testLong(19876543210L)
        testLong(119876543210L)
        testLong(1119876543210L)
        testLong(111119876543210L)
        testLong(1111119876543210L)
        testLong(99876543210L)
        testLong(999876543210L)
        testLong(9999876543210L)
        testLong(99999876543210L)
        testLong(999999876543210L)
        testLong(89876543210L)
        testLong(889876543210L)
        testLong(79876543210L)
        testLong(779876543210L)
        testLong(69876543210L)
        testLong(669876543210L)
        testLong(9151314442816847872L)
    }

    @Test
    fun testInts() {
        testInt(0)
        testInt(1)
        testInt(987654321)
        testInt(123456789)
        testInt(1111111111)
        testInt(222222222)
        testInt(333333333)
        testInt(444444444)
        testInt(555555555)
        testInt(666666666)
        testInt(777777777)
        testInt(888888888)
        testInt(999999999)
    }

    @Test
    fun testCurrentTime() {
        val time = System.currentTimeMillis()
        val output = LightOutputStream()
        writeCompressed(output, time)
        Assert.assertTrue(output.size() < 8)
    }

    @Test
    fun testSuccessive() {
        var prev = longToCompressedEntry(0)
        for (i in 1L..999999L) {
            val current = longToCompressedEntry(i)
            Assert.assertTrue(current.compareTo(prev) > 0)
            prev = current
        }
    }

    @Test
    fun testLongMaxValue() {
        Assert.assertEquals(java.lang.Long.MAX_VALUE, compressedEntryToLong(longToCompressedEntry(java.lang.Long.MAX_VALUE)))
        Assert.assertEquals(java.lang.Long.MAX_VALUE - 1, compressedEntryToLong(longToCompressedEntry(java.lang.Long.MAX_VALUE - 1)))
    }

    @Test
    fun testSuccessive2() {
        var prev = intToCompressedEntry(0)
        for (i in 1..999999) {
            val current = intToCompressedEntry(i)
            Assert.assertTrue(current.compareTo(prev) > 0)
            prev = current
        }
    }

    @Test
    @TestFor(issues = arrayOf("XD-537"))
    fun testSignedLongs() {
        Assert.assertEquals(1, signedLongToCompressedEntry(-1L).length.toLong())
        assertSignedLong(-1L)
        Assert.assertEquals(1, signedLongToCompressedEntry(-8L).length.toLong())
        assertSignedLong(-8L)
        Assert.assertEquals(2, signedLongToCompressedEntry(-9L).length.toLong())
        assertSignedLong(-9L)
        Assert.assertEquals(2, signedLongToCompressedEntry(-2048L).length.toLong())
        assertSignedLong(-2048L)
        Assert.assertEquals(3, signedLongToCompressedEntry(-2049L).length.toLong())
        assertSignedLong(-2049L)
        assertSignedLong((java.lang.Long.MAX_VALUE * (Math.random() - 0.5)).toLong())
    }

    @Test
    @TestFor(issues = arrayOf("XD-537"))
    fun testSignedInts() {
        Assert.assertEquals(1, signedIntToCompressedEntry(-1).length.toLong())
        assertSignedInt(-1)
        Assert.assertEquals(1, signedIntToCompressedEntry(-16).length.toLong())
        assertSignedInt(-16)
        Assert.assertEquals(2, signedIntToCompressedEntry(-17).length.toLong())
        assertSignedInt(-17)
        Assert.assertEquals(2, signedIntToCompressedEntry(-4096).length.toLong())
        assertSignedInt(-4096)
        Assert.assertEquals(3, signedIntToCompressedEntry(-4097).length.toLong())
        assertSignedInt(-4097)
        assertSignedInt((Integer.MAX_VALUE * (Math.random() - 0.5)).toInt())
    }

    private fun assertSignedLong(value: Long) {
        Assert.assertEquals(value, compressedEntryToSignedLong(signedLongToCompressedEntry(value)))
    }

    private fun assertSignedInt(value: Int) {
        Assert.assertEquals(value.toLong(), compressedEntryToSignedInt(signedIntToCompressedEntry(value)).toLong())
    }

    private fun testLong(l: Long) {
        val output = LightOutputStream()
        writeCompressed(output, l)
        val input = output.asArrayByteIterable()
        Assert.assertEquals(l, LongBinding.readCompressed(input.iterator()))
    }

    private fun testInt(i: Int) {
        val output = LightOutputStream()
        writeCompressed(output, i)
        val input = output.asArrayByteIterable()
        Assert.assertEquals(i.toLong(), IntegerBinding.readCompressed(input.iterator()).toLong())
    }
}
