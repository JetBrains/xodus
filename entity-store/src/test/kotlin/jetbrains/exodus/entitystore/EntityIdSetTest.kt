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

import jetbrains.exodus.entitystore.util.EntityIdSetFactory
import jetbrains.exodus.entitystore.util.ImmutableSingleTypeEntityIdBitSet
import org.junit.Assert
import java.util.*

class EntityIdSetTest : EntityStoreTestBase() {

    fun testEntityIdSet() {
        var set = EntityIdSetFactory.newSet()
        for (i in 0..9) {
            for (j in 0L..999L) {
                set = set.add(i, j)
            }
            for (j in 0L..999L) {
                Assert.assertTrue(set.contains(i, j))
            }
            for (j in 0L..999L) {
                Assert.assertFalse(set.contains(i + 1, j))
            }
        }
        Assert.assertEquals(-1, set.count().toLong())
        Assert.assertFalse(set.contains(null))
        set = set.add(null)
        Assert.assertTrue(set.contains(null))
    }

    fun testEntityIdSetIterator() {
        var set = EntityIdSetFactory.newSet()
        for (i in 0..9) {
            for (j in 0L..999L) {
                set = set.add(i, j)
            }
        }
        var sample = EntityIdSetFactory.newSet()
        for (entityId in set) {
            Assert.assertTrue(set.contains(entityId))
            Assert.assertFalse(sample.contains(entityId))
            sample = sample.add(entityId)
        }
        Assert.assertFalse(sample.contains(null))
        set = set.add(null)
        sample = EntityIdSetFactory.newSet()
        for (entityId in set) {
            Assert.assertTrue(set.contains(entityId))
            Assert.assertFalse(sample.contains(entityId))
            sample = sample.add(entityId)
        }
        Assert.assertTrue(sample.contains(null))
    }

    fun testEntityIdSetCount() {
        var set = EntityIdSetFactory.newSet()
        for (i in 0..99) {
            set = set.add(7, i.toLong())
        }
        Assert.assertEquals(100, set.count().toLong())
    }

    fun testBitSet() {
        checkSet(0, 2, 7, 11, 55, 78)
        checkSet(4, 6, 7, 11, 55, 260)
        checkSet(4147483647L, 4147483648L, 4147483649L)
    }

    private fun checkSet(vararg data: Long) {
        Arrays.sort(data)
        val typeId = 7
        val set = ImmutableSingleTypeEntityIdBitSet(typeId, data)
        assertEquals(data.size, set.count())
        var prevValue = data[0] - 20
        for (i in data.indices) {
            val value = data[i]
            for (k in prevValue + 1 until value) {
                assertFalse(set.contains(typeId, k))
                assertEquals(-1, set.indexOf(PersistentEntityId(typeId, k)))
            }
            prevValue = value
            assertTrue(set.contains(typeId, value))
            assertFalse(set.contains(3, value))
            assertEquals(i, set.indexOf(PersistentEntityId(typeId, value)))
        }
        val actualData = LongArray(data.size)
        var i = 0
        for (id in set) {
            assertEquals(typeId, id.typeId)
            actualData[i++] = id.localId
        }
        Assert.assertArrayEquals(data, actualData)
        val reverseIterator = set.reverseIterator()
        while (reverseIterator.hasNext()) {
            val next = reverseIterator.next()
            Assert.assertEquals(data[--i], next.localId)
        }
        Assert.assertEquals(data[0], set.first.localId)
        Assert.assertEquals(data[data.size - 1], set.last.localId)
    }
}
