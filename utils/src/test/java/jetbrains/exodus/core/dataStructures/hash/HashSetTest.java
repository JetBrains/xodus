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
import java.util.Set;

public class HashSetTest {

    @Test
    public void testAddContains() {
        final HashSet<Integer> tested = new HashSet<>();
        for (int i = 0; i < 1000; ++i) {
            tested.add(i);
        }
        Assert.assertEquals(1000, tested.size());
        for (int i = 0; i < 1000; ++i) {
            Assert.assertTrue(tested.contains(i));
        }
    }

    @Test
    public void testAddContainsRemove() {
        final HashSet<Integer> tested = new HashSet<>();
        for (int i = 0; i < 1000; ++i) {
            tested.add(i);
        }
        Assert.assertEquals(1000, tested.size());
        for (int i = 0; i < 1000; i += 2) {
            Assert.assertTrue(tested.remove(i));
        }
        Assert.assertEquals(500, tested.size());
        for (int i = 0; i < 1000; ++i) {
            if (i % 2 == 0) {
                Assert.assertFalse(tested.contains(i));
            } else {
                Assert.assertTrue(tested.contains(i));
            }
        }
    }

    @Test
    public void nulls() {
        final Set<Integer> tested = new HashSet<>();
        Assert.assertTrue(tested.add(null));
        Assert.assertFalse(tested.add(null));
        Assert.assertTrue(tested.contains(null));
        Assert.assertEquals(1, tested.size());
        Assert.assertTrue(tested.remove(null));
        Assert.assertEquals(0, tested.size());
    }

    @Test
    public void nulls2() {
        final Set<Integer> tested = new HashSet<>();
        Assert.assertTrue(tested.add(null));
        Assert.assertFalse(tested.add(null));
        Assert.assertTrue(tested.contains(null));
        Assert.assertEquals(1, tested.size());
        Assert.assertTrue(tested.add(1));
        Assert.assertFalse(tested.add(1));
        Assert.assertEquals(2, tested.size());
        boolean hasNull = false;
        for (Integer integer : tested) {
            if (integer == null) {
                hasNull = true;
                break;
            }
        }
        Assert.assertTrue(hasNull);
        Assert.assertTrue(tested.remove(null));
        Assert.assertEquals(1, tested.size());
    }

    @Test
    public void iterator() {
        final HashSet<Integer> tested = new HashSet<>();
        final Set<Integer> set = new java.util.HashSet<>();

        for (int i = 0; i < 10000; ++i) {
            tested.add(i);
            set.add(i);
        }
        for (Integer key : tested) {
            Assert.assertTrue(set.remove(key));
        }
        Assert.assertEquals(0, set.size());
    }

    @Test
    public void iterator2() {
        final HashSet<Integer> tested = new HashSet<>();

        for (int i = 0; i < 10000; ++i) {
            tested.add(i);
        }
        Iterator<Integer> it = tested.iterator();
        while (it.hasNext()) {
            final int i = it.next();
            if (i % 2 == 0) {
                it.remove();
            }
        }

        Assert.assertEquals(5000, tested.size());

        it = tested.iterator();
        for (int i = 9999; i > 0; i -= 2) {
            Assert.assertTrue(it.hasNext());
            Assert.assertTrue(it.next() % 2 != 0);
        }
    }
}
