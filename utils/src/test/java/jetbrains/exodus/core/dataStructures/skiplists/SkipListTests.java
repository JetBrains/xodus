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

import jetbrains.exodus.core.dataStructures.skiplists.SkipList.SkipListNode;
import jetbrains.exodus.util.Random;
import org.junit.Assert;
import org.junit.Test;

public class SkipListTests {

    private static final int BENCHMARK_SIZE = 2000000;
    private static final int MAX_KEY = BENCHMARK_SIZE - 1;

    @Test
    public void testClearList() {
        final SkipList<Integer> list = new SkipList<>();
        for (int i = 0; i < 1000; i++) {
            list.add(i);
        }
        Assert.assertEquals(1000, list.size());
        Integer min = list.getMinimum();
        Assert.assertEquals(0, min.intValue());
        Integer max = list.getMaximum();
        Assert.assertEquals(999, max.intValue());
        list.clear();
        Assert.assertEquals(0, list.size());
        Assert.assertNull(list.getMinimum());
        Assert.assertNull(list.getMaximum());
    }

    @Test
    public void testGetMinimumMaximum() throws InterruptedException {
        final SkipList<Integer> list = new SkipList<>();
        for (int i = 0; i < BENCHMARK_SIZE; i++) {
            list.add(i);
        }
        System.gc();
        System.out.print("Memory used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        Integer min = list.getMinimum();
        Assert.assertEquals(0, min.intValue());
        Integer max = list.getMaximum();
        Assert.assertEquals(MAX_KEY, max.intValue());
    }

    @Test
    public void testGetMinimumMaximum2() throws InterruptedException {
        final SkipList<Integer> list = new SkipList<>();
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        Random rnd = new Random();
        for (int i = 0; i < BENCHMARK_SIZE; i++) {
            final int key = rnd.nextInt();
            list.add(key);
            if (key > max) {
                max = key;
            }
            if (key < min) {
                min = key;
            }
        }
        System.gc();
        System.out.print("Memory used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        Assert.assertEquals(min, list.getMinimum().intValue());
        Assert.assertEquals(max, list.getMaximum().intValue());
    }

    @Test
    public void testGetMinimumMaximum3() throws InterruptedException {
        final SkipList<Integer> list = new SkipList<>();
        for (int i = BENCHMARK_SIZE - 1; i >= 0; --i) {
            list.add(i);
        }
        System.gc();
        System.out.print("Memory used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        Integer min = list.getMinimum();
        Assert.assertEquals(0, min.intValue());
        Integer max = list.getMaximum();
        Assert.assertEquals(MAX_KEY, max.intValue());
    }

    @Test
    public void testSearch() throws InterruptedException {
        final SkipList<Integer> list = new SkipList<>();
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
        final SkipList<Integer> list = new SkipList<>();
        for (int i = 0; i < 10000; ++i) {
            list.add(i);
        }
        int last = -1;
        SkipListNode<Integer> node = list.getMinimumNode();
        while (node != null) {
            int current = node.getKey();
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
    public void testFakeRootEmpty() {
        final SkipList<Integer> list = new SkipList<>();
        Assert.assertNull(list.getFakeRoot().getNext());
    }

    @Test
    public void testBackwardIteration() {
        final SkipList<Integer> list = new SkipList<>();
        for (int i = 0; i < 10000; ++i) {
            list.add(i);
        }
        int last = Integer.MAX_VALUE;
        SkipListNode<Integer> node = list.getMaximumNode();
        while (node != null) {
            int current = node.getKey();
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
        final SkipList<Integer> list = new SkipList<>();
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
        final SkipList<Integer> list = new SkipList<>();
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
        final SkipList<Integer> list = new SkipList<>();
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
        final SkipList<Integer> list = new SkipList<>();
        for (int i = 0; i < 1000; i += 2) {
            list.add(i);
            list.add(i);
        }
        for (int i = 1; i < 1000; i += 2) {
            list.add(i);
            list.add(i);
        }
        Assert.assertEquals(2000, list.size());
        SkipListNode<Integer> node = list.getMinimumNode();
        for (int i = 0; i < 1000; i++) {
            assert node != null;
            Assert.assertEquals(i, node.getKey().intValue());
            node = list.getNext(node);
            Assert.assertEquals(i, node.getKey().intValue());
            node = list.getNext(node);
        }
        Assert.assertNull(node);
        node = list.getMaximumNode();
        for (int i = 0; i < 1000; i++) {
            assert node != null;
            Assert.assertEquals(999 - i, node.getKey().intValue());
            node = list.getPrevious(node);
            Assert.assertEquals(999 - i, node.getKey().intValue());
            node = list.getPrevious(node);
        }
        Assert.assertNull(node);
        for (int i = 0; i < 1000; i++) {
            Assert.assertTrue(list.remove(i));
        }
        Assert.assertEquals(1000, list.size());
        node = list.getMinimumNode();
        for (int i = 0; i < 1000; i++) {
            assert node != null;
            Assert.assertEquals(i, node.getKey().intValue());
            node = list.getNext(node);
        }
        Assert.assertNull(node);
        node = list.getMaximumNode();
        for (int i = 0; i < 1000; i++) {
            assert node != null;
            Assert.assertEquals(999 - i, node.getKey().intValue());
            node = list.getPrevious(node);
        }
        Assert.assertNull(node);
    }

    @Test
    public void testCount() {
        SkipList<Integer> list = new SkipList<>();
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
        SkipList<String> list = new SkipList<>();
        list.add("charisma0");
        SkipListNode<String> node = list.getGreaterOrEqual("charisma1");
        Assert.assertNull(node);
        list.add("charisma1");
        list.add("charisma2");
        node = list.getGreaterOrEqual("charisma3");
        Assert.assertNull(node);
        list.add("charisma3");
        node = list.getGreaterOrEqual("charisma4");
        Assert.assertNull(node);
        node = list.getGreaterOrEqual("charisma1");
        assert node != null;
        Assert.assertEquals("charisma1", node.getKey());
        node = list.getNext(node);
        assert node != null;
        Assert.assertEquals("charisma2", node.getKey());
        node = list.getNext(node);
        assert node != null;
        Assert.assertEquals("charisma3", node.getKey());
        node = list.getNext(node);
        Assert.assertNull(node);
    }

    @Test
    public void testGetEqualOrGreater2() {
        SkipList<String> list = new SkipList<>();
        list.add("charisma0");
        list.add("charisma4");
        list.add("charisma2");
        list.add("charisma3");
        SkipListNode<String> node = list.getGreaterOrEqual("charisma1");
        assert node != null;
        Assert.assertEquals("charisma2", node.getKey());
        node = list.getNext(node);
        assert node != null;
        Assert.assertEquals("charisma3", node.getKey());
        node = list.getNext(node);
        assert node != null;
        Assert.assertEquals("charisma4", node.getKey());
        node = list.getNext(node);
        Assert.assertNull(node);
    }

    @Test
    public void testGetEqualOrGreater3() {
        SkipList<TestObjectForEqualOrGreater> list = new SkipList<>();
        list.add(new TestObjectForEqualOrGreater("charisma0", 0));
        list.add(new TestObjectForEqualOrGreater("charisma4", 4));
        list.add(new TestObjectForEqualOrGreater("charisma2", 2));
        list.add(new TestObjectForEqualOrGreater("charisma1", 5));
        list.add(new TestObjectForEqualOrGreater("charisma3", 3));
        list.add(new TestObjectForEqualOrGreater("charisma1", 6));
        list.add(new TestObjectForEqualOrGreater("charisma1", 7));

        SkipListNode<TestObjectForEqualOrGreater> node = list.getGreaterOrEqual(new TestObjectForEqualOrGreater("charisma1", 0));
        assert node != null;
        Assert.assertEquals("charisma1", node.getKey().getStr());
        Assert.assertEquals(5, node.getKey().getNum());

        node = list.getNext(node);

        assert node != null;
        Assert.assertEquals("charisma1", node.getKey().getStr());
        Assert.assertEquals(6, node.getKey().getNum());

        node = list.getNext(node);

        assert node != null;
        Assert.assertEquals("charisma1", node.getKey().getStr());
        Assert.assertEquals(7, node.getKey().getNum());
    }

    @Test
    public void testGetEqualOrGreater4() {
        SkipList<Integer> list = new SkipList<>();
        for (int i = 0; i < 100000; i++) {
            list.add(i);
        }

        for (int i = 10000; i < 10010; i++) {
            list.remove(i);
            SkipListNode<Integer> node = list.getGreaterOrEqual(i);
            assert node != null;
            Assert.assertEquals(i + 1, node.getKey().intValue());
            for (int j = i + 2; j < 100000; j++) {
                node = list.getNext(node);
                assert node != null;
                Assert.assertEquals(j, node.getKey().intValue());
            }
        }
    }

    @Test
    public void testGetNextAndPrevious() {
        SkipList<TestObjectForEqualOrGreater> list = new SkipList<>();
        list.add(new TestObjectForEqualOrGreater("charisma0", 0));
        list.add(new TestObjectForEqualOrGreater("charisma2", 2));
        list.add(new TestObjectForEqualOrGreater("charisma1", 1));
        list.add(new TestObjectForEqualOrGreater("charisma3", 3));

        SkipListNode<TestObjectForEqualOrGreater> node = list.getGreaterOrEqual(new TestObjectForEqualOrGreater("charisma0", 0));
        valuesTest(node, "charisma0", 0);
        node = list.getNext(node);

        valuesTest(node, "charisma1", 1);
        node = list.getNext(node);

        valuesTest(node, "charisma2", 2);
        node = list.getNext(node);

        valuesTest(node, "charisma3", 3);
        Assert.assertNull(list.getNext(node));

        node = list.getPrevious(node);
        valuesTest(node, "charisma2", 2);

        node = list.getPrevious(node);
        valuesTest(node, "charisma1", 1);

        node = list.getPrevious(node);
        valuesTest(node, "charisma0", 0);

        Assert.assertNull(list.getPrevious(node));
    }

    @Test
    public void testGetEqualOrLess() {
        SkipList<TestObjectForEqualOrGreater> list = new SkipList<>();
        list.add(new TestObjectForEqualOrGreater("charisma1", 1));
        list.add(new TestObjectForEqualOrGreater("charisma0", 0));
        list.add(new TestObjectForEqualOrGreater("charisma2", 2));
        list.add(new TestObjectForEqualOrGreater("charisma3", 3));

        SkipListNode<TestObjectForEqualOrGreater> node = list.getLessOrEqual(new TestObjectForEqualOrGreater("charisma1", 0));
        valuesTest(node, "charisma1", 1);
        node = list.getNext(node);

        valuesTest(node, "charisma2", 2);
        node = list.getNext(node);

        valuesTest(node, "charisma3", 3);
    }

    @Test
    public void testGetEqualOrLess2() {
        SkipList<TestObjectForEqualOrGreater> list = new SkipList<>();
        list.add(new TestObjectForEqualOrGreater("charisma0", 0));
        list.add(new TestObjectForEqualOrGreater("charisma2", 2));
        list.add(new TestObjectForEqualOrGreater("charisma3", 3));

        SkipListNode<TestObjectForEqualOrGreater> node = list.getLessOrEqual(new TestObjectForEqualOrGreater("charisma1", 0));
        valuesTest(node, "charisma0", 0);
        node = list.getNext(node);

        valuesTest(node, "charisma2", 2);
        node = list.getNext(node);

        valuesTest(node, "charisma3", 3);
    }

    @Test
    public void testGetEqualOrLess3() {
        SkipList<TestObjectForEqualOrGreater> list = new SkipList<>();
        list.add(new TestObjectForEqualOrGreater("charisma0", 0));
        list.add(new TestObjectForEqualOrGreater("charisma0", 0));
        list.add(new TestObjectForEqualOrGreater("charisma0", 0));

        SkipListNode<TestObjectForEqualOrGreater> node = list.getLessOrEqual(new TestObjectForEqualOrGreater("charisma0", 0));
        valuesTest(node, "charisma0", 0);
        node = list.getNext(node);

        valuesTest(node, "charisma0", 0);
        node = list.getNext(node);

        valuesTest(node, "charisma0", 0);
    }

    @Test
    public void testGetEqualOrLess4() {
        SkipList<TestObjectForEqualOrGreater> list = new SkipList<>();
        list.add(new TestObjectForEqualOrGreater("charisma0", 0));
        list.add(new TestObjectForEqualOrGreater("charisma1", 1));
        list.add(new TestObjectForEqualOrGreater("charisma1", 1));
        list.add(new TestObjectForEqualOrGreater("charisma1", 1));
        list.add(new TestObjectForEqualOrGreater("charisma1", 1));
        list.add(new TestObjectForEqualOrGreater("charisma1", 1));
        list.add(new TestObjectForEqualOrGreater("charisma1", 1));
        list.add(new TestObjectForEqualOrGreater("charisma2", 2));

        SkipListNode<TestObjectForEqualOrGreater> node = list.getLessOrEqual(new TestObjectForEqualOrGreater("charisma1", 1));
        valuesTest(node, "charisma1", 1);
        node = list.getNext(node);

        valuesTest(node, "charisma1", 1);
        node = list.getNext(node);

        valuesTest(node, "charisma1", 1);
        node = list.getNext(node);

        valuesTest(node, "charisma1", 1);
        node = list.getNext(node);

        valuesTest(node, "charisma1", 1);
        node = list.getNext(node);

        valuesTest(node, "charisma1", 1);
        node = list.getNext(node);
        valuesTest(node, "charisma2", 2);
    }

    @Test
    public void testGetEqualOrLess5() {
        SkipList<TestObjectForEqualOrGreater> list = new SkipList<>();
        list.add(new TestObjectForEqualOrGreater("charisma0", 0));
        list.add(new TestObjectForEqualOrGreater("charisma0", 0));
        list.add(new TestObjectForEqualOrGreater("charisma0", 0));
        list.add(new TestObjectForEqualOrGreater("charisma2", 2));
        list.add(new TestObjectForEqualOrGreater("charisma2", 2));
        list.add(new TestObjectForEqualOrGreater("charisma2", 2));

        SkipListNode<TestObjectForEqualOrGreater> node = list.getLessOrEqual(new TestObjectForEqualOrGreater("charisma1", 1));
        valuesTest(node, "charisma0", 0);
        node = list.getNext(node);

        valuesTest(node, "charisma2", 2);
        node = list.getNext(node);

        valuesTest(node, "charisma2", 2);
    }

    @Test
    public void testStressTestForGetMinimum() {
        SkipList<TestObject> list = new SkipList<>();
        for (int i = 0; i < 100000; i++) {
            list.add(new TestObject(4));
        }
        list.add(new TestObject(1));
        SkipListNode<TestObject> node;
        int key = 0;
        while (key != 1) {
            node = list.getMinimumNode();
            assert node != null;
            key = ((TestObject) node.getKey())._value;
            list.remove(new TestObject(key));
        }
    }

    private class TestObject implements Comparable<TestObject> {
        public final int _value;

        private TestObject(int value) {
            _value = value;
        }

        @Override
        public int compareTo(TestObject o) {
            return o._value - _value;
        }
    }

    private class TestObjectForEqualOrGreater implements Comparable<TestObjectForEqualOrGreater> {
        private final String _str;
        private final int _num;

        private TestObjectForEqualOrGreater(String str, int num) {
            _str = str;
            _num = num;
        }

        @Override
        public int compareTo(TestObjectForEqualOrGreater o) {
            return _str.compareTo(o._str);
        }

        public String getStr() {
            return _str;
        }

        public int getNum() {
            return _num;
        }
    }

    private void valuesTest(SkipListNode<TestObjectForEqualOrGreater> node, String str, int num) {
        Assert.assertEquals(str, ((TestObjectForEqualOrGreater) node.getKey()).getStr());
        Assert.assertEquals(num, ((TestObjectForEqualOrGreater) node.getKey()).getNum());
    }
}
