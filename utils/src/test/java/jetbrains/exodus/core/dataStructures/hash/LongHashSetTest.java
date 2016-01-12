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
package jetbrains.exodus.core.dataStructures.hash;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;

public class LongHashSetTest {

    @Test
    public void testAddContains() {
        final Set<Long> tested = new LongHashSet();
        for (int i = 0; i < 1000; ++i) {
            tested.add(i + 100000000000L);
        }
        Assert.assertEquals(1000, tested.size());
        for (int i = 0; i < 1000; ++i) {
            Assert.assertTrue(tested.contains(i + 100000000000L));
        }
    }

    @Test
    public void testAddContainsRemove() {
        final Set<Long> tested = new LongHashSet();
        for (int i = 0; i < 1000; ++i) {
            tested.add(i + 100000000000L);
        }
        Assert.assertEquals(1000, tested.size());
        for (int i = 0; i < 1000; i += 2) {
            Assert.assertTrue(tested.remove(i + 100000000000L));
        }
        Assert.assertEquals(500, tested.size());
        for (int i = 0; i < 1000; ++i) {
            if (i % 2 == 0) {
                Assert.assertFalse(tested.contains(i + 100000000000L));
            } else {
                Assert.assertTrue(tested.contains(i + 100000000000L));
            }
        }
    }

    @Test
    public void iterator() {
        final Set<Long> tested = new LongHashSet();
        final Set<Long> set = new java.util.HashSet<>();

        for (long i = 0; i < 10000; ++i) {
            tested.add(i);
            set.add(i);
        }
        for (Long key : tested) {
            Assert.assertTrue(set.remove(key));
        }
        Assert.assertEquals(0, set.size());
    }

    @Test
    public void iterator2() {
        final Set<Long> tested = new LongHashSet();
        final Set<Long> set = new HashSet<>();

        for (long i = 0; i < 10000; ++i) {
            tested.add(i);
            set.add(i);
        }
        Iterator<Long> it = tested.iterator();
        while (it.hasNext()) {
            final long i = it.next();
            if (i % 2 == 0) {
                it.remove();
                Assert.assertTrue(set.remove(i));
            }
        }

        Assert.assertEquals(5000, tested.size());

        it = tested.iterator();
        for (long i = 9999; i > 0; i -= 2) {
            Assert.assertTrue(it.hasNext());
            Assert.assertTrue(it.next() % 2 != 0);
            Assert.assertTrue(set.remove(i));
        }
        Assert.assertEquals(0, set.size());
    }
}
