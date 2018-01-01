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
package jetbrains.exodus.core.dataStructures.persistent;

import org.junit.Assert;
import org.junit.Test;

import java.util.NoSuchElementException;

public class PersistentStackTest {

    @Test
    public void testAddRemove() {
        PersistentStack<Integer> stack = PersistentStack.EMPTY_STACK;
        Assert.assertTrue(stack.isEmpty());
        try {
            stack.skip();
            Assert.fail();
        } catch (NoSuchElementException e) {
        }
        for (int i = 0; i < 9000; i++) {
            stack = stack.push(i);
            Assert.assertFalse(stack.isEmpty());
            Assert.assertEquals(Integer.valueOf(i), stack.peek());
            Assert.assertEquals(i + 1, stack.size());
        }
        for (int i = 8999; i >= 0; i--) {
            Assert.assertFalse(stack.isEmpty());
            Assert.assertEquals(Integer.valueOf(i), stack.peek());
            PersistentStack<Integer> next = stack.skip();
            Assert.assertEquals(next, stack.skip());
            stack = next;
        }
        Assert.assertTrue(stack.isEmpty());
        try {
            stack.skip();
            Assert.fail();
        } catch (NoSuchElementException e) {
        }
    }
}
