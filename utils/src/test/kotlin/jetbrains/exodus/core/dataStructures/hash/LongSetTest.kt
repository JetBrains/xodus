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
package jetbrains.exodus.core.dataStructures.hash

import org.junit.Assert
import org.junit.Test

abstract class LongSetTest {

    @Test
    fun testAddContains() {
        val tested = newSet()
        (0..999).mapTo(tested) { it + 100000000000L }
        Assert.assertEquals(1000, tested.size)
        for (i in 0..999) {
            Assert.assertTrue(tested.contains(i + 100000000000L))
        }
    }

    @Test
    fun testAddContainsRemove() {
        val tested = newSet()
        (0..999).mapTo(tested) { it + 100000000000L }
        Assert.assertEquals(1000, tested.size)
        run {
            var i = 0
            while (i < 1000) {
                Assert.assertTrue(tested.remove(i + 100000000000L))
                i += 2
            }
        }
        Assert.assertEquals(500, tested.size)
        for (i in 0..999) {
            if (i % 2 == 0) {
                Assert.assertFalse(tested.contains(i + 100000000000L))
            } else {
                Assert.assertTrue(tested.contains(i + 100000000000L))
            }
        }
    }

    @Test
    operator fun iterator() {
        val tested = newSet()
        val set = java.util.HashSet<Long>()

        for (i in 0L..9999L) {
            tested.add(i)
            set.add(i)
        }
        for (key in tested) {
            Assert.assertTrue(set.remove(key))
        }
        Assert.assertEquals(0, set.size)
    }

    @Test
    fun iterator2() {
        val tested = newSet()
        val set = HashSet<Long>()

        for (i in 0L..9999L) {
            tested.add(i)
            set.add(i)
        }
        var it: MutableIterator<Long> = tested.iterator()
        while (it.hasNext()) {
            val i = it.next()
            if (i % 2 == 0L) {
                it.remove()
                Assert.assertTrue(set.remove(i))
            }
        }

        Assert.assertEquals(5000, tested.size)

        it = tested.iterator()
        var i: Long = 9999
        while (i > 0) {
            Assert.assertTrue(it.hasNext())
            Assert.assertTrue(it.next() % 2 != 0L)
            Assert.assertTrue(set.remove(i))
            i -= 2
        }
        Assert.assertEquals(0, set.size)
    }

    abstract fun newSet(): LongSet
}