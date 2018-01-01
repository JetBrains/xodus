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
package jetbrains.exodus.core.dataStructures.hash;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class LongLinkedHashSetTest {

    @Test
    public void testAddContains() {
        final LongLinkedHashSet tested = new LongLinkedHashSet();
        for (long i = 0; i < 1000; ++i) {
            tested.add(i);
        }
        Assert.assertEquals(1000, tested.size());
        for (long i = 0; i < 1000; ++i) {
            Assert.assertTrue(tested.contains(i));
        }
    }

    @Test
    public void testAddContainsRemove() {
        final LongLinkedHashSet tested = new LongLinkedHashSet();
        for (long i = 0; i < 1000; ++i) {
            tested.add(i);
        }
        Assert.assertEquals(1000, tested.size());
        for (long i = 0; i < 1000; i += 2) {
            Assert.assertTrue(tested.remove(i));
        }
        Assert.assertEquals(500, tested.size());
        for (long i = 0; i < 1000; ++i) {
            if (i % 2 == 0) {
                Assert.assertFalse(tested.contains(i));
            } else {
                Assert.assertTrue(tested.contains(i));
            }
        }
    }

    @Test
    public void iterator() {
        final LongLinkedHashSet tested = new LongLinkedHashSet();

        for (long i = 0; i < 10000; ++i) {
            tested.add(i);
        }
        long i = 0;
        for (Long key : tested) {
            Assert.assertEquals(i++, key.longValue());
            tested.remove(key);
        }
        Assert.assertEquals(0, tested.size());
    }

    @Test
    public void iterator2() {
        final LongLinkedHashSet tested = new LongLinkedHashSet();
        for (long i = 0; i < 10000; ++i) {
            tested.add(i);
        }
        Iterator<Long> it = tested.iterator();
        while (it.hasNext()) {
            final long i = it.next();
            if (i % 2 == 0) {
                it.remove();
            }
        }

        Assert.assertEquals(5000, tested.size());

        it = tested.iterator();
        for (long i = 1; i < 10000; i += 2) {
            Assert.assertTrue(it.hasNext());
            Assert.assertEquals(i, it.next().longValue());
        }
    }
}
