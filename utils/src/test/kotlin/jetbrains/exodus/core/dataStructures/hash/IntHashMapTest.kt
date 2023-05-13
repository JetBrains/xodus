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

class IntHashMapTest {
    @Test
    fun testPutGet() {
        val tested: MutableMap<Int, String> = IntHashMap()
        for (i in 0..999) {
            tested[i] = Integer.toString(i)
        }
        Assert.assertEquals(1000, tested.size.toLong())
        for (i in 0..999) {
            Assert.assertEquals(Integer.toString(i), tested[i])
        }
        for (i in 0..999) {
            Assert.assertEquals(Integer.toString(i), tested.put(i, Integer.toString(i + 1)))
        }
        Assert.assertEquals(1000, tested.size.toLong())
        for (i in 0..999) {
            Assert.assertEquals(Integer.toString(i + 1), tested[i])
        }
    }

    @Test
    fun testPutGet2() {
        val tested: MutableMap<Int, String> = IntHashMap()
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
        val tested: MutableMap<Int, String> = IntHashMap()
        for (i in 0..999) {
            tested[i] = Integer.toString(i)
        }
        Assert.assertEquals(1000, tested.size.toLong())
        run {
            var i = 0
            while (i < 1000) {
                Assert.assertEquals(Integer.toString(i), tested.remove(i))
                i += 2
            }
        }
        Assert.assertEquals(500, tested.size.toLong())
        for (i in 0..999) {
            Assert.assertEquals(if (i % 2 == 0) null else Integer.toString(i), tested[i])
        }
    }

    @Test
    fun keySet() {
        val tested: MutableMap<Int, String> = IntHashMap()
        val set: MutableSet<Int> = IntHashSet()
        for (i in 0..9999) {
            tested[i] = Integer.toString(i)
            set.add(i)
        }
        for (key in tested.keys) {
            Assert.assertTrue(set.remove(key))
        }
        Assert.assertEquals(0, set.size.toLong())
    }

    @Test
    fun keySet2() {
        val tested: MutableMap<Int, String> = IntHashMap()
        val set: MutableSet<Int> = IntHashSet()
        for (i in 0..9999) {
            tested[i] = Integer.toString(i)
            set.add(i)
        }
        var it = tested.keys.iterator()
        while (it.hasNext()) {
            val i = it.next()
            if (i % 2 == 0) {
                it.remove()
                Assert.assertTrue(set.remove(i))
            }
        }
        Assert.assertEquals(5000, tested.size.toLong())
        it = tested.keys.iterator()
        var i = 9999
        while (i > 0) {
            Assert.assertTrue(it.hasNext())
            Assert.assertTrue(it.next() % 2 != 0)
            Assert.assertTrue(set.remove(i))
            i -= 2
        }
        Assert.assertEquals(0, set.size.toLong())
    }

    @Test
    fun forEachProcedure() {
        val tested = IntHashMap<String>()
        for (i in 0..99999) {
            tested.put(i, Integer.toString(i))
        }
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
        tested.forEachKey { `object`: Int ->
            ii[0]++
            `object` < 500
        }
        tested.forEachValue { `object`: String? ->
            ii[0]++
            true
        }
        Assert.assertEquals((tested.size + 501).toLong(), ii[0].toLong())
    }
}
