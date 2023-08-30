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
package jetbrains.exodus.diskann.util.collections;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class NonBlockingHashMapLongLongBasicTest {
    static private NonBlockingHashMapLongLong nbhmll;

    @BeforeClass
    public static void setUp() {
        nbhmll = new NonBlockingHashMapLongLong();
    }

    @AfterClass
    public static void tearDown() {
        nbhmll = null;
    }

    // Test some basic stuff; add a few keys, remove a few keys
    @Test
    public void testBasic() {
        Assert.assertEquals(0, nbhmll.size());
        Assert.assertEquals(-1, nbhmll.put(1, 101));
        checkSizes(1);
        Assert.assertEquals(-1, nbhmll.putIfAbsent(2, 102));
        checkSizes(2);
        Assert.assertTrue(nbhmll.containsKey(2));
        Assert.assertEquals(101, nbhmll.put(1, 103));
        Assert.assertEquals(102, nbhmll.put(2, 104));
        checkSizes(2);
        Assert.assertEquals(104, nbhmll.putIfAbsent(2, 105));
        Assert.assertEquals(103, nbhmll.remove(1));
        Assert.assertFalse(nbhmll.containsKey(1));
        checkSizes(1);
        Assert.assertEquals(-1, nbhmll.remove(1));
        Assert.assertEquals(104, nbhmll.remove(2));
        checkSizes(0);
        Assert.assertEquals(-1, nbhmll.remove(2));
        Assert.assertEquals(0, nbhmll.size());

        Assert.assertEquals(-1, nbhmll.put(0, 0));
        Assert.assertTrue(nbhmll.containsKey(0));
        checkSizes(1);
        Assert.assertEquals(0, nbhmll.remove(0));
        Assert.assertFalse(nbhmll.containsKey(0));
        checkSizes(0);

        Assert.assertEquals(-1, nbhmll.replace(0, 1));
        Assert.assertFalse(nbhmll.containsKey(0));
        Assert.assertEquals(-1, nbhmll.put(0, 1));
        Assert.assertEquals(1, nbhmll.replace(0, 2));
        Assert.assertEquals(2, nbhmll.get(0));
        Assert.assertEquals(2, nbhmll.remove(0));
        Assert.assertFalse(nbhmll.containsKey(0));
        checkSizes(0);

        Assert.assertEquals(-1, nbhmll.replace(1, 200));
        Assert.assertFalse(nbhmll.containsKey(1));
        Assert.assertEquals(-1, nbhmll.put(1, 200));
        Assert.assertEquals(200, nbhmll.replace(1, 300));
        Assert.assertEquals(300, nbhmll.get(1));
        Assert.assertEquals(300, nbhmll.remove(1));
        Assert.assertFalse(nbhmll.containsKey(1));
        checkSizes(0);

        // Simple insert of simple keys, with no reprobing on insert until the
        // table gets full exactly.  Then do a 'get' on the totally full table.
        NonBlockingHashMapLongLong map = new NonBlockingHashMapLongLong(32);
        for (int i = 1; i < 32; i++) {
            map.put(i, i);
        }

        Assert.assertEquals(-1, map.get(33));
    }

    @Test
    public void replaceMissingValue() {
        NonBlockingHashMapLongLong map = new NonBlockingHashMapLongLong();
        Assert.assertEquals(-1, map.replace(1, 2));
        Assert.assertFalse(map.replace(1, 2, 3));
    }

    // Check all iterators for correct size counts
    private void checkSizes(int expectedSize) {
        Assert.assertEquals("size()", expectedSize, nbhmll.size());
    }

    @Test
    public void testConcurrentSimple() throws InterruptedException {
        final NonBlockingHashMapLongLong nbhml = new NonBlockingHashMapLongLong();

        // In 2 threads, add & remove even & odd elements concurrently
        final int num_thrds = 2;
        Thread[] ts = new Thread[num_thrds];
        for (int i = 1; i < num_thrds; i++) {
            final int x = i;
            ts[i] = new Thread(() -> work_helper(nbhml, x));
        }
        for (int i = 1; i < num_thrds; i++) {
            ts[i].start();
        }
        work_helper(nbhml, 0);
        for (int i = 1; i < num_thrds; i++) {
            ts[i].join();
        }

        Assert.assertEquals(0, nbhml.size());
    }

    void work_helper(NonBlockingHashMapLongLong nbhml, int d) {
        final int ITERS = 20000;
        for (int j = 0; j < 10; j++) {
            for (int i = d; i < ITERS; i += 2) {
                Assert.assertEquals(-1, nbhml.putIfAbsent(i, d));
            }
            for (int i = d; i < ITERS; i += 2) {
                Assert.assertTrue(nbhml.remove(i, d));
            }
        }
    }

}
