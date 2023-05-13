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
package jetbrains.exodus.core.dataStructures.hash

import org.junit.Assert
import org.junit.Test

class LongLinkedHashMapTest {
    @Test
    fun testPutGet() {
        val tested = LongLinkedHashMap<String>()
        for (i in 0..999) {
            tested.put(i.toLong(), java.lang.Long.toString(i.toLong()))
        }
        Assert.assertEquals(1000, tested.size.toLong())
        for (i in 0..999) {
            Assert.assertEquals(java.lang.Long.toString(i.toLong()), tested.get(i.toLong()))
        }
        for (i in 0..999) {
            Assert.assertEquals(
                java.lang.Long.toString(i.toLong()), tested.put(
                    i.toLong(), java.lang.Long.toString(
                        (i + 1).toLong()
                    )
                )
            )
        }
        Assert.assertEquals(1000, tested.size.toLong())
        for (i in 0..999) {
            Assert.assertEquals(java.lang.Long.toString((i + 1).toLong()), tested.get(i.toLong()))
        }
    }

    @Test
    fun testPutGet2() {
        val tested = LongLinkedHashMap<String>()
        for (i in 0..999) {
            tested.put((i - 500).toLong(), java.lang.Long.toString(i.toLong()))
        }
        Assert.assertEquals(1000, tested.size.toLong())
        for (i in 0..999) {
            Assert.assertEquals(java.lang.Long.toString(i.toLong()), tested.get(i.toLong() - 500))
        }
        for (i in 0..999) {
            Assert.assertEquals(
                java.lang.Long.toString(i.toLong()), tested.put(
                    (i - 500).toLong(), java.lang.Long.toString(
                        (i + 1).toLong()
                    )
                )
            )
        }
        Assert.assertEquals(1000, tested.size.toLong())
        for (i in 0..999) {
            Assert.assertEquals(java.lang.Long.toString((i + 1).toLong()), tested.get(i.toLong() - 500))
        }
    }

    @Test
    fun testPutGetRemove() {
        val tested = LongLinkedHashMap<String>()
        for (i in 0..999) {
            tested.put(i.toLong(), java.lang.Long.toString(i.toLong()))
        }
        Assert.assertEquals(1000, tested.size.toLong())
        run {
            var i: Long = 0
            while (i < 1000) {
                Assert.assertEquals(java.lang.Long.toString(i), tested.remove(i))
                i += 2
            }
        }
        Assert.assertEquals(500, tested.size.toLong())
        for (i in 0L..999) {
            Assert.assertEquals(if (i % 2 == 0L) null else java.lang.Long.toString(i.toLong()), tested.get(i))
        }
    }

    @Test
    fun keySet() {
        val tested = LongLinkedHashMap<String>()
        for (i in 0..9999) {
            tested.put(i.toLong(), java.lang.Long.toString(i.toLong()))
        }
        var i: Long = 10000
        for (key in tested.keys) {
            Assert.assertEquals(--i, key)
        }
    }

    @Test
    fun keySet2() {
        val tested = LongLinkedHashMap<String>()
        for (i in 0..9999) {
            tested.put(i.toLong(), java.lang.Long.toString(i.toLong()))
        }
        var it = tested.keys.iterator()
        while (it.hasNext()) {
            val i = it.next()
            if (i % 2 == 0L) {
                it.remove()
            }
        }
        Assert.assertEquals(5000, tested.size.toLong())
        it = tested.keys.iterator()
        var i: Long = 9999
        while (i > 0) {
            Assert.assertTrue(it.hasNext())
            Assert.assertEquals(i, it.next())
            i -= 2
        }
    }

    @Test
    fun lru() {
        val tested: LongLinkedHashMap<String> = object : LongLinkedHashMap<String>() {
            override fun removeEldestEntry(eldest: Map.Entry<Long, String>): Boolean {
                return size > 500
            }
        }
        for (i in 0..999) {
            tested.put(i.toLong(), java.lang.Long.toString(i.toLong()))
        }
        Assert.assertEquals(500, tested.size.toLong())
        for (i in 0..499) {
            Assert.assertNull(tested.remove(i.toLong()))
        }
        Assert.assertEquals(500, tested.size.toLong())
        for (i in 500..999) {
            Assert.assertEquals(java.lang.Long.toString(i.toLong()), tested.remove(i.toLong()))
        }
        Assert.assertEquals(0, tested.size.toLong())
    }

    @Test
    fun lru2() {
        val tested: LongLinkedHashMap<String> = object : LongLinkedHashMap<String>() {
            override fun removeEldestEntry(eldest: Map.Entry<Long, String>): Boolean {
                return size > 1000
            }
        }
        for (i in 0..999) {
            tested.put(i.toLong(), java.lang.Long.toString(i.toLong()))
        }
        Assert.assertEquals(java.lang.Long.toString(0), tested[0])
        for (i in 1000..1998) {
            tested.put(i.toLong(), java.lang.Long.toString(i.toLong()))
        }
        Assert.assertEquals(java.lang.Long.toString(0), tested[0])
        tested.put(2000, java.lang.Long.toString(2000))
        Assert.assertNull(tested[1000])
    }

    @Test
    fun lru3() {
        val tested: LongLinkedHashMap<String> = object : LongLinkedHashMap<String>() {
            override fun removeEldestEntry(eldest: Map.Entry<Long, String>): Boolean {
                return size > 1000
            }
        }
        for (i in 0..999) {
            tested.put(i.toLong(), java.lang.Long.toString(i.toLong()))
        }
        Assert.assertEquals(java.lang.Long.toString(999), tested.remove(999))
        Assert.assertEquals(999, tested.size.toLong())
        Assert.assertEquals(java.lang.Long.toString(0), tested[0])
        for (i in 1000..1998) {
            tested.put(i.toLong(), java.lang.Long.toString(i.toLong()))
        }
        Assert.assertEquals(java.lang.Long.toString(0), tested[0])
        tested.put(2000, java.lang.Long.toString(2000))
        Assert.assertNull(tested[1000])
    }

    @Test
    fun forEachProcedure() {
        val tested = LongLinkedHashMap<String>()
        for (i in 0..99999) {
            tested.put(i.toLong(), java.lang.Long.toString(i.toLong()))
        }
        val ii = intArrayOf(0)
        tested.forEachKey { `object`: Long? ->
            ii[0]++
            true
        }
        tested.forEachValue { `object`: String? ->
            ii[0]++
            true
        }
        Assert.assertEquals((tested.size * 2).toLong(), ii[0].toLong())
        ii[0] = 0
        tested.forEachKey { `object`: Long ->
            ii[0]++
            `object` > 99500
        }
        tested.forEachValue { `object`: String? ->
            ii[0]++
            true
        }
        Assert.assertEquals((tested.size + 500).toLong(), ii[0].toLong())
    }
}
