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

class HashMapTest {
    @Test
    fun testPutGet() {
        val tested: MutableMap<Int?, String> = HashMap()
        for (i in 0..999) {
            tested[i] = Integer.toString(i)
        }
        tested[null] = "null"
        Assert.assertEquals(1001, tested.size.toLong())
        for (i in 0..999) {
            Assert.assertEquals(Integer.toString(i), tested[i])
        }
        Assert.assertEquals("null", tested[null])
        for (i in 0..999) {
            Assert.assertEquals(Integer.toString(i), tested.put(i, Integer.toString(i + 1)))
        }
        Assert.assertEquals("null", tested.put(null, "new null"))
        Assert.assertEquals(1001, tested.size.toLong())
        for (i in 0..999) {
            Assert.assertEquals(Integer.toString(i + 1), tested[i])
        }
        Assert.assertEquals("new null", tested[null])
    }

    @Test
    fun testPutGet2() {
        val tested: MutableMap<Int, String> = HashMap()
        for (i in 0..999) {
            tested[i - 500] = Integer.toString(i)
        }
        Assert.assertEquals(1000, tested.size.toLong())
        for (i in 0..999) {
            Assert.assertEquals(Integer.toString(i), tested[i - 500])
        }
        for (i in 0..999) {
            Assert.assertEquals(Integer.toString(i), tested.put(i - 500, Integer.toString(i + 1)))
        }
        Assert.assertEquals(1000, tested.size.toLong())
        for (i in 0..999) {
            Assert.assertEquals(Integer.toString(i + 1), tested[i - 500])
        }
    }

    @Test
    fun testPutGetRemove() {
        val tested: MutableMap<Int?, String> = HashMap()
        for (i in 0..999) {
            tested[i] = Integer.toString(i)
        }
        tested[null] = "null"
        Assert.assertEquals(1001, tested.size.toLong())
        run {
            var i = 0
            while (i < 1000) {
                Assert.assertEquals(Integer.toString(i), tested.remove(i))
                i += 2
            }
        }
        Assert.assertEquals(501, tested.size.toLong())
        Assert.assertEquals("null", tested[null])
        tested.remove(null)
        Assert.assertNull(tested[null])
        for (i in 0..999) {
            Assert.assertEquals(if (i % 2 == 0) null else Integer.toString(i), tested[i])
        }
    }

    @Test
    fun keySet() {
        val tested: MutableMap<Int?, String> = HashMap()
        val set: MutableSet<Int?> = HashSet()
        for (i in 0..9999) {
            tested[i] = Integer.toString(i)
            set.add(i)
        }
        tested[null] = "null"
        set.add(null)
        for (key in tested.keys) {
            Assert.assertTrue(set.remove(key))
        }
        Assert.assertEquals(0, set.size.toLong())
    }

    @Test
    fun keySet2() {
        val tested: MutableMap<Int?, String> = HashMap()
        val set: MutableSet<Int?> = HashSet()
        for (i in 0..9999) {
            tested[i] = Integer.toString(i)
            set.add(i)
        }
        tested[null] = "null"
        set.add(null)
        var it = tested.keys.iterator()
        while (it.hasNext()) {
            val i = it.next()
            it.remove()
            Assert.assertTrue(set.remove(i))
            if (it.hasNext()) {
                it.next()
            }
        }
        Assert.assertEquals(5000, tested.size.toLong())
        it = tested.keys.iterator()
        var i = 9998
        while (i >= 0) {
            Assert.assertTrue(it.hasNext())
            Assert.assertEquals(0, (it.next()!! % 2).toLong())
            Assert.assertTrue(set.remove(i))
            i -= 2
        }
        Assert.assertEquals(0, set.size.toLong())
    }

    @Test
    fun testCopy() {
        val tested = HashMap<Int, String>()
        tested[7] = "a"
        tested[8] = "b"
        val copy = HashMap(tested)
        Assert.assertEquals("a", copy[7])
        Assert.assertEquals("b", copy[8])
        Assert.assertEquals(2, copy.size.toLong())
    }

    @Test
    fun testCopyAndModify() {
        val tested = HashMap<Int, String>()
        tested[7] = "a"
        tested[8] = "b"
        val copy = HashMap(tested)
        tested[7] = "c"
        Assert.assertEquals("a", copy[7])
        Assert.assertEquals("b", copy[8])
        Assert.assertEquals(2, copy.size.toLong())
    }

    @Test
    fun forEachProcedure() {
        val tested = HashMap<Int, String>()
        for (i in 0..99999) {
            tested[i] = Integer.toString(i)
        }
        tested[null] = "null"
        val ii = intArrayOf(0)
        tested.forEachKey { `object`: Int? ->
            ii[0]++
            true
        }
        tested.forEachValue { `object`: String? ->
            ii[0]++
            true
        }
        Assert.assertEquals((tested.size * 2).toLong(), ii[0].toLong())
        ii[0] = 0
        tested.forEachKey { `object`: Int? ->
            ii[0]++
            `object` == null || `object` < 500
        }
        tested.forEachValue { `object`: String? ->
            ii[0]++
            true
        }
        Assert.assertEquals((tested.size + 502).toLong(), ii[0].toLong())
    }
}
