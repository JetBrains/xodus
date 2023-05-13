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
package jetbrains.exodus.core.dataStructures.persistent

import org.junit.Assert
import org.junit.Test

class PersistentQueueTest {
    @Test
    fun testAddRemove() {
        var queue = PersistentQueue.EMPTY
        Assert.assertTrue(queue.isEmpty)
        try {
            queue.skip()
            Assert.fail()
        } catch (e: NoSuchElementException) {
        }
        for (i in 0..99) {
            queue = queue.add(i)
            Assert.assertFalse(queue.isEmpty)
            Assert.assertEquals(Integer.valueOf(0), queue.peek())
            Assert.assertEquals((i + 1).toLong(), queue.size().toLong())
        }
        for (i in 0..99) {
            Assert.assertFalse(queue.isEmpty)
            Assert.assertEquals(Integer.valueOf(i), queue.peek())
            val next = queue.skip()
            Assert.assertEquals(next, queue.skip())
            queue = next
        }
        Assert.assertTrue(queue.isEmpty)
        try {
            queue.skip()
            Assert.fail()
        } catch (e: NoSuchElementException) {
        }
    }
}
