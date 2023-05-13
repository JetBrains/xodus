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

class PersistentStackTest {
    @Test
    fun testAddRemove() {
        var stack = PersistentStack.EMPTY_STACK
        Assert.assertTrue(stack.isEmpty)
        try {
            stack.skip()
            Assert.fail()
        } catch (e: NoSuchElementException) {
        }
        for (i in 0..8999) {
            stack = stack.push(i)
            Assert.assertFalse(stack.isEmpty)
            Assert.assertEquals(Integer.valueOf(i), stack.peek())
            Assert.assertEquals((i + 1).toLong(), stack.size().toLong())
        }
        for (i in 8999 downTo 0) {
            Assert.assertFalse(stack.isEmpty)
            Assert.assertEquals(Integer.valueOf(i), stack.peek())
            val next = stack.skip()
            Assert.assertEquals(next, stack.skip())
            stack = next
        }
        Assert.assertTrue(stack.isEmpty)
        try {
            stack.skip()
            Assert.fail()
        } catch (e: NoSuchElementException) {
        }
    }
}
