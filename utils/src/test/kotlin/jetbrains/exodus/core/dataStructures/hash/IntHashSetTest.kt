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

class IntHashSetTest {
    @Test
    fun testAddContains() {
        val tested: MutableSet<Int> = IntHashSet()
        for (i in 0..999) {
            tested.add(i)
        }
        Assert.assertEquals(1000, tested.size.toLong())
        for (i in 0..999) {
            Assert.assertTrue(tested.contains(i))
        }
    }

    @Test
    fun testAddContainsRemove() {
        val tested: MutableSet<Int> = IntHashSet()
        for (i in 0..999) {
            tested.add(i)
        }
        Assert.assertEquals(1000, tested.size.toLong())
        run {
            var i = 0
            while (i < 1000) {
                Assert.assertTrue(tested.remove(i))
                i += 2
            }
        }
        Assert.assertEquals(500, tested.size.toLong())
        for (i in 0..999) {
            if (i % 2 == 0) {
                Assert.assertFalse(tested.contains(i))
            } else {
                Assert.assertTrue(tested.contains(i))
            }
        }
    }

    @Test
    operator fun iterator() {
        val tested: MutableSet<Int> = IntHashSet()
        val set: MutableSet<Int> = java.util.HashSet()
        for (i in 0..9999) {
            tested.add(i)
            set.add(i)
        }
        for (key in tested) {
            Assert.assertTrue(set.remove(key))
        }
        Assert.assertEquals(0, set.size.toLong())
    }

    @Test
    fun iterator2() {
        val tested: MutableSet<Int> = IntHashSet()
        val set: MutableSet<Int> = HashSet()
        for (i in 0..9999) {
            tested.add(i)
            set.add(i)
        }
        var it = tested.iterator()
        while (it.hasNext()) {
            val i = it.next()
            if (i % 2 == 0) {
                it.remove()
                Assert.assertTrue(set.remove(i))
            }
        }
        Assert.assertEquals(5000, tested.size.toLong())
        it = tested.iterator()
        var i = 9999
        while (i > 0) {
            Assert.assertTrue(it.hasNext())
            Assert.assertTrue(it.next() % 2 != 0)
            Assert.assertTrue(set.remove(i))
            i -= 2
        }
        Assert.assertEquals(0, set.size.toLong())
    }
}
