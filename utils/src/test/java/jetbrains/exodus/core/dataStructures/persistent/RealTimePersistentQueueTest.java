/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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

import jetbrains.exodus.core.dataStructures.persistent.trial.RealTimePersistentQueue;
import org.junit.Assert;
import org.junit.Test;

public class RealTimePersistentQueueTest {

    @Test
    public void testAddRemovePasha() {
        RealTimePersistentQueue<String> queue = RealTimePersistentQueue.EMPTY;
        for (int i = 0; i < 9000; i++) {
            queue = queue.add("test " + i);
        }
        int count = 0;
        for (final String s : queue) {
            Assert.assertEquals("test " + count, s);
            count++;
        }
        Assert.assertEquals(9000, count);
        Assert.assertEquals(9000, queue.getSize());
        for (int i = 0; i < 9000; i++) {
            queue = queue.pop();
        }
        Assert.assertEquals(0, queue.getSize());
        Assert.assertFalse(queue.iterator().hasNext());
        Assert.assertTrue(queue.peek() == null);
        Assert.assertEquals(queue, queue.pop());
    }
}
