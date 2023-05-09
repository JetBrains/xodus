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
package jetbrains.exodus.log

import jetbrains.exodus.*
import jetbrains.exodus.ByteIterator
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getIterable
import jetbrains.exodus.log.CompressedUnsignedLongByteIterable.Companion.getLong
import jetbrains.exodus.log.NullLoggable.create
import jetbrains.exodus.util.Random
import org.junit.Assert
import org.junit.Test
import kotlin.math.abs

class LoggableTests {
    @Test
    fun testCompressedLongByteIterator() {
        Assert.assertTrue(matchesArray(getIterable(0), byteArrayOf(-128)))
        Assert.assertTrue(matchesArray(getIterable(1), byteArrayOf(-127)))
        Assert.assertTrue(matchesArray(getIterable(128), byteArrayOf(0, -127)))
        Assert.assertTrue(matchesArray(getIterable(16384), byteArrayOf(0, 0, -127)))
        Assert.assertTrue(matchesArray(getIterable(16383), byteArrayOf(127, -1)))
        Assert.assertTrue(matchesArray(getIterable(25000), byteArrayOf(40, 67, -127)))
        Assert.assertTrue(
            matchesArray(
                getIterable(Long.MAX_VALUE),
                byteArrayOf(127, 127, 127, 127, 127, 127, 127, 127, -1)
            )
        )
    }

    @Test
    fun testCompressedLongByteIterator2() {
        val rnd = Random()
        for (i in 0..9999) {
            val l = abs(rnd.nextLong())
            Assert.assertEquals(l, getLong(getIterable(l)))
        }
    }

    @Test
    fun testFactoryNullLoggable() {
        val nullLoggable: Loggable = create()
        Assert.assertNotNull(nullLoggable)
        Assert.assertEquals(nullLoggable.getType().toLong(), create().getType().toLong())
    }

    @Test
    fun testCompoundByteIterable1() {
        Assert.assertTrue(
            matchesArray(
                CompoundByteIterable(
                    arrayOf<ByteIterable>(
                        ArrayByteIterable(byteArrayOf())
                    )
                ), byteArrayOf()
            )
        )
        Assert.assertTrue(
            matchesArray(
                CompoundByteIterable(
                    arrayOf<ByteIterable>(
                        ArrayByteIterable(byteArrayOf()),
                        ArrayByteIterable(byteArrayOf())
                    )
                ), byteArrayOf()
            )
        )
    }

    @Test
    fun testCompoundByteIterable2() {
        Assert.assertTrue(
            matchesArray(
                CompoundByteIterable(
                    arrayOf<ByteIterable>(
                        ArrayByteIterable(byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
                    )
                ), byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
            )
        )
        Assert.assertTrue(
            matchesArray(
                CompoundByteIterable(
                    arrayOf<ByteIterable>(
                        ArrayByteIterable(byteArrayOf()),
                        ArrayByteIterable(byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
                    )
                ), byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
            )
        )
        Assert.assertTrue(
            matchesArray(
                CompoundByteIterable(
                    arrayOf<ByteIterable>(
                        ArrayByteIterable(byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)),
                        ArrayByteIterable(byteArrayOf())
                    )
                ), byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
            )
        )
        Assert.assertTrue(
            matchesArray(
                CompoundByteIterable(
                    arrayOf<ByteIterable>(
                        ArrayByteIterable(byteArrayOf()),
                        ArrayByteIterable(byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)),
                        ArrayByteIterable(byteArrayOf())
                    )
                ), byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
            )
        )
    }

    @Test
    fun testCompoundByteIterable3() {
        val ci = CompoundByteIterable(
            arrayOf<ByteIterable>(
                ArrayByteIterable(byteArrayOf(0, 1, 2, 3, 4)),
                ArrayByteIterable(byteArrayOf(5, 6, 7, 8, 9))
            )
        )
        Assert.assertEquals(10, ci.length.toLong())
        Assert.assertTrue(
            matchesArray(
                ci, byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
            )
        )
    }

    @Test
    fun testCompoundByteIteratorSkip2() {
        val iterator: CompoundByteIteratorBase = object : CompoundByteIteratorBase() {
            val a = arrayOf(byteArrayOf(1), byteArrayOf(2), byteArrayOf(3), byteArrayOf(4))
            var current = 0
            override fun nextIterator(): ByteIterator {
                return ArrayByteIterable(a[current++]).iterator()
            }
        }
        iterator.skip(3)
        Assert.assertEquals(iterator.next().toLong(), 4)
    }

    @Test
    fun testCompoundByteIterator() {
        Assert.assertTrue(matchesArray(object : ByteIterableBase() {
            override fun getIterator(): ByteIterator {
                return object : CompoundByteIteratorBase() {
                    var array1: ByteArray? = byteArrayOf(0, 1, 2, 3)
                    var array2: ByteArray? = byteArrayOf(4, 5, 6, 7, 8, 9)
                    override fun nextIterator(): ByteIterator? {
                        val a1 = array1
                        if (a1 != null) {
                            array1 = null
                            return ArrayByteIterable(a1).iterator()
                        }
                        val a2 = array2
                        if (a2 != null) {
                            array2 = null
                            return ArrayByteIterable(a2).iterator()
                        }
                        return null
                    }
                }
            }
        }, byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
    }

    @Test
    fun testCompoundByteIterator2() {
        Assert.assertTrue(matchesArray(object : ByteIterableBase() {
            override fun getIterator(): ByteIterator {
                return object : CompoundByteIteratorBase() {
                    var array1: ByteArray? = byteArrayOf()
                    var array2: ByteArray? = byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
                    override fun nextIterator(): ByteIterator? {
                        val a1 = array1
                        if (a1 != null) {
                            array1 = null
                            return ArrayByteIterable(a1).iterator()
                        }
                        val a2 = array2
                        if (a2 != null) {
                            array2 = null
                            return ArrayByteIterable(a2).iterator()
                        }
                        return null
                    }
                }
            }
        }, byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
    }

    @Test
    fun testCompoundByteIterator3() {
        Assert.assertTrue(matchesArray(object : ByteIterableBase() {
            override fun getIterator(): ByteIterator {
                return object : CompoundByteIteratorBase() {
                    var array1: ByteArray? = byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
                    var array2: ByteArray? = byteArrayOf()
                    override fun nextIterator(): ByteIterator? {
                        val a1 = array1
                        if (a1 != null) {
                            array1 = null
                            return ArrayByteIterable(a1).iterator()
                        }
                        val a2 = array2
                        if (a2 != null) {
                            array2 = null
                            return ArrayByteIterable(a2).iterator()
                        }
                        return null
                    }
                }
            }
        }, byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
    }

    @Test
    fun testCompoundByteIteratorN() {
        Assert.assertTrue(
            matchesArray(
                CompoundByteArrayIterable(
                    arrayOf(
                        ArrayByteIterable(byteArrayOf(0, 1, 2)).iterator(),
                        ArrayByteIterable(byteArrayOf(3, 4, 5)).iterator(),
                        ArrayByteIterable(byteArrayOf(6, 7, 8, 9)).iterator(),
                        ArrayByteIterable(ByteArray(0)).iterator()
                    )
                ), byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
            )
        )
    }

    @Test
    fun testCompoundByteIteratorSkip() {
        val it = CompoundByteArrayIterable(
            arrayOf(
                ArrayByteIterable(byteArrayOf(0, 1, 2)).iterator(),
                ArrayByteIterable(byteArrayOf(3, 4, 5)).iterator(),
                ArrayByteIterable(byteArrayOf(6, 7, 8, 9)).iterator(),
                ArrayByteIterable(ByteArray(0)).iterator()
            )
        ).iterator()
        // skip first two elements
        it.next()
        it.next()
        Assert.assertEquals(2L, it.skip(2))
        Assert.assertEquals(4.toByte().toLong(), it.next().toLong())
        Assert.assertEquals(5L, it.skip(6))
    }

    private fun matchesArray(iterable: ByteIterable, array: ByteArray): Boolean {
        val iterator = iterable.iterator()
        var i = 0
        while (iterator.hasNext()) {
            if (i == array.size) {
                return false
            }
            val b = iterator.next()
            if (b != array[i]) {
                println(i.toString() + ": " + b + " != " + array[i])
                return false
            }
            ++i
        }
        return i == array.size
    }

    private class CompoundByteArrayIterable(private val iterators: Array<ByteIterator>) : ByteIterableBase() {
        private val count: Int = iterators.size
        private val offset = 0

        override fun getIterator(): ByteIterator {
            return object : CompoundByteIteratorBase(iterators[0]) {
                var off = offset
                public override fun nextIterator(): ByteIterator? {
                    return if (off < count) iterators[off++] else null
                }
            }
        }
    }
}
