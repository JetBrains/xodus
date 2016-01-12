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
package jetbrains.exodus.core.dataStructures.skiplists;

import jetbrains.exodus.util.Random;
import org.junit.Assert;
import org.junit.Test;

import java.util.SortedSet;
import java.util.TreeSet;

public class LongSkipListTests {

    private static final int BENCHMARK_SIZE = 2000000;
    private static final long MAX_KEY = BENCHMARK_SIZE - 1;

    @Test
    public void testClearList() {
        final LongSkipList list = new LongSkipList();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }
        Assert.assertEquals(1000, list.size());
        Assert.assertEquals(0L, list.getMinimum().longValue());
        Assert.assertEquals(999L, list.getMaximum().longValue());
        list.clear();
        Assert.assertEquals(0, list.size());
        Assert.assertNull(list.getMinimum());
        Assert.assertNull(list.getMaximum());
    }

    @Test
    public void testGetMinimumMaximum() {
        final LongSkipList list = new LongSkipList();
        for (int i = 0; i < BENCHMARK_SIZE; i++) {
            list.add(i);
        }
        Assert.assertEquals(0L, list.getMinimum().longValue());
        Assert.assertEquals(MAX_KEY, list.getMaximum().longValue());
        System.out.print("Memory used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    }

    @Test
    public void testGetMinimumMaximum2() {
        final LongSkipList list = new LongSkipList();
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        Random rnd = new Random();
        for (int i = 0; i < BENCHMARK_SIZE; i++) {
            final long key = rnd.nextLong();
            list.add(key);
            if (key > max) {
                max = key;
            }
            if (key < min) {
                min = key;
            }
        }
        Assert.assertEquals(min, list.getMinimum().longValue());
        Assert.assertEquals(max, list.getMaximum().longValue());
        System.out.print("Memory used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    }

    @Test
    public void testGetMinimumMaximumTreeSet() {
        final SortedSet<Long> set = new TreeSet<>();
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        Random rnd = new Random();
        for (int i = 0; i < BENCHMARK_SIZE; i++) {
            final long key = rnd.nextLong();
            set.add(key);
            if (key > max) {
                max = key;
            }
            if (key < min) {
                min = key;
            }
        }
        Assert.assertEquals(min, set.first().longValue());
        Assert.assertEquals(max, set.last().longValue());
        System.out.print("Memory used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    }

    @Test
    public void testGetMinimumMaximum3() {
        final LongSkipList list = new LongSkipList();
        for (int i = BENCHMARK_SIZE - 1; i >= 0; --i) {
            list.add(i);
        }
        Assert.assertEquals(0L, list.getMinimum().longValue());
        Assert.assertEquals(MAX_KEY, list.getMaximum().longValue());
        System.out.print("Memory used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    }

    @Test
    public void testSearch() {
        final LongSkipList list = new LongSkipList();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }
        for (int i = 1999; i >= 1000; i--) {
            list.add(i);
        }
        for (int i = 0; i < 2000; i++) {
            Assert.assertNotNull(list.search(i));
        }
        Assert.assertNull(list.search(-1));
    }

    @Test
    public void testForwardIteration() {
        final LongSkipList list = new LongSkipList();
        for (int i = 0; i < 10000; ++i) {
            list.add(i);
        }
        long last = -1;
        LongSkipList.SkipListNode node = list.getMinimumNode();
        while (node != null) {
            long current = node.getKey();
            Assert.assertTrue(current > last);
            last = current;
            node = list.getNext(node);
        }
        for (int i = 0; i < 10000; ++i) {
            node = list.getMinimumNode();
            while (node != null) {
                node = list.getNext(node);
            }
        }
    }

    @Test
    public void testBackwardIteration() {
        final LongSkipList list = new LongSkipList();
        for (int i = 0; i < 10000; ++i) {
            list.add(i);
        }
        long last = Long.MAX_VALUE;
        LongSkipList.SkipListNode node = list.getMaximumNode();
        while (node != null) {
            long current = node.getKey();
            Assert.assertTrue(current < last);
            last = current;
            node = list.getPrevious(node);
        }
        for (int i = 0; i < 10000; ++i) {
            node = list.getMaximumNode();
            while (node != null) {
                node = list.getPrevious(node);
            }
        }
    }

    @Test
    public void testDeletingForwardOrder() {
        final LongSkipList list = new LongSkipList();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }
        for (int i = 0; i < 1000; i++) {
            Assert.assertTrue(list.remove(i));
            Assert.assertFalse(list.remove(i));
        }
        Assert.assertEquals(0, list.size());
    }

    @Test
    public void testDeletingBackOrder() {
        final LongSkipList list = new LongSkipList();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }
        for (int i = 999; i >= 0; i--) {
            Assert.assertTrue(list.remove(i));
            Assert.assertFalse(list.remove(i));
        }
        Assert.assertEquals(0, list.size());
    }

    @Test
    public void testDeleting() {
        final LongSkipList list = new LongSkipList();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
            list.add(i);
        }
        Assert.assertEquals(2000, list.size());
        for (int i = 0; i < 1000; i++) {
            Assert.assertTrue(list.remove(i));
        }
        Assert.assertEquals(1000, list.size());
        for (int i = 0; i < 1000; i++) {
            Assert.assertTrue(list.remove(i));
        }
        Assert.assertEquals(0, list.size());
    }

    @Test
    public void testDeletingIteration() {
        final LongSkipList list = new LongSkipList();
        for (int i = 0; i < 1000; i += 2) {
            list.add(i);
            list.add(i);
        }
        for (int i = 1; i < 1000; i += 2) {
            list.add(i);
            list.add(i);
        }
        Assert.assertEquals(2000, list.size());
        LongSkipList.SkipListNode node = list.getMinimumNode();
        for (long i = 0; i < 1000; i++) {
            assert node != null;
            Assert.assertEquals(i, node.getKey());
            node = list.getNext(node);
            Assert.assertEquals(i, node.getKey());
            node = list.getNext(node);
        }
        Assert.assertNull(node);
        node = list.getMaximumNode();
        for (long i = 0; i < 1000; i++) {
            assert node != null;
            Assert.assertEquals(999 - i, node.getKey());
            node = list.getPrevious(node);
            Assert.assertEquals(999 - i, node.getKey());
            node = list.getPrevious(node);
        }
        Assert.assertNull(node);
        for (int i = 0; i < 1000; i++) {
            Assert.assertTrue(list.remove(i));
        }
        Assert.assertEquals(1000, list.size());
        node = list.getMinimumNode();
        for (long i = 0; i < 1000; i++) {
            assert node != null;
            Assert.assertEquals(i, node.getKey());
            node = list.getNext(node);
        }
        Assert.assertNull(node);
        node = list.getMaximumNode();
        for (long i = 0; i < 1000; i++) {
            assert node != null;
            Assert.assertEquals(999L - i, node.getKey());
            node = list.getPrevious(node);
        }
        Assert.assertNull(node);
    }

    @Test
    public void testCount() {
        LongSkipList list = new LongSkipList();
        list.add(3);
        Assert.assertEquals(1, list.size());
        list.remove(3);
        Assert.assertEquals(0, list.size());
        list.add(2);
        list.add(4);
        list.add(5);
        list.add(6);
        list.add(7);
        list.add(8);
        list.add(9);
        list.add(10);
        list.add(11);
        list.add(12);
        Assert.assertEquals(10, list.size());
        list.remove(8);
        Assert.assertEquals(9, list.size());
        list.remove(4);
        Assert.assertEquals(8, list.size());
        list.remove(2);
        Assert.assertEquals(7, list.size());
        list.remove(10);
        Assert.assertEquals(6, list.size());
        list.remove(12);
        Assert.assertEquals(5, list.size());
        list.remove(6);
        Assert.assertEquals(4, list.size());
        list.remove(7);
        Assert.assertEquals(3, list.size());
        list.remove(5);
        Assert.assertEquals(2, list.size());
        list.remove(11);
        Assert.assertEquals(1, list.size());
        list.remove(9);
        Assert.assertEquals(0, list.size());
    }

    @Test
    public void testGetEqualOrGreater() {
        LongSkipList list = new LongSkipList();
        list.add(0L);
        LongSkipList.SkipListNode node = list.getGreaterOrEqual(1L);
        Assert.assertNull(node);
        list.add(1L);
        list.add(2L);
        node = list.getGreaterOrEqual(3L);
        Assert.assertNull(node);
        list.add(3L);
        node = list.getGreaterOrEqual(4L);
        Assert.assertNull(node);
        node = list.getGreaterOrEqual(1L);
        assert node != null;
        Assert.assertEquals(1L, node.getKey());
        node = list.getNext(node);
        assert node != null;
        Assert.assertEquals(2L, node.getKey());
        node = list.getNext(node);
        assert node != null;
        Assert.assertEquals(3L, node.getKey());
        node = list.getNext(node);
        Assert.assertNull(node);
    }

    @Test
    public void testGetEqualOrGreater2() {
        LongSkipList list = new LongSkipList();
        list.add(0);
        list.add(4);
        list.add(2);
        list.add(3);
        LongSkipList.SkipListNode node = list.getGreaterOrEqual(1);
        assert node != null;
        Assert.assertEquals(2L, node.getKey());
        node = list.getNext(node);
        assert node != null;
        Assert.assertEquals(3L, node.getKey());
        node = list.getNext(node);
        assert node != null;
        Assert.assertEquals(4L, node.getKey());
        node = list.getNext(node);
        Assert.assertNull(node);
    }
}
