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

class LongLinkedHashSetTest {
    @Test
    fun testAddContains() {
        val tested = LongLinkedHashSet()
        for (i in 0..999) {
            tested.add(i.toLong())
        }
        Assert.assertEquals(1000, tested.size.toLong())
        for (i in 0..999) {
            Assert.assertTrue(tested.contains(i.toLong()))
        }
    }

    @Test
    fun testAddContainsRemove() {
        val tested = LongLinkedHashSet()
        for (i in 0..999) {
            tested.add(i.toLong())
        }
        Assert.assertEquals(1000, tested.size.toLong())
        run {
            var i: Long = 0
            while (i < 1000) {
                Assert.assertTrue(tested.remove(i))
                i += 2
            }
        }
        Assert.assertEquals(500, tested.size.toLong())
        for (i in 0L..999) {
            if (i % 2 == 0L) {
                Assert.assertFalse(tested.contains(i))
            } else {
                Assert.assertTrue(tested.contains(i))
            }
        }
    }

    @Test
    operator fun iterator() {
        val tested = LongLinkedHashSet()
        for (i in 0..9999) {
            tested.add(i.toLong())
        }
        var i: Long = 0
        for (key in tested) {
            Assert.assertEquals(i++, key)
            tested.remove(key)
        }
        Assert.assertEquals(0, tested.size.toLong())
    }

    @Test
    fun iterator2() {
        val tested = LongLinkedHashSet()
        for (i in 0..9999) {
            tested.add(i.toLong())
        }
        var it: MutableIterator<Long> = tested.iterator()
        while (it.hasNext()) {
            val i = it.next()
            if (i % 2 == 0L) {
                it.remove()
            }
        }
        Assert.assertEquals(5000, tested.size.toLong())
        it = tested.iterator()
        var i: Long = 1
        while (i < 10000) {
            Assert.assertTrue(it.hasNext())
            Assert.assertEquals(i, it.next())
            i += 2
        }
    }
}
