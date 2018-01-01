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

import jetbrains.exodus.util.Random;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class Persistent23TreeMapTest {

    private static final int ENTRIES_TO_ADD = 5000;

    @Test
    public void mutableTreeRandomInsertDeleteTest() {
        Random random = new Random(2343489);
        Persistent23TreeMap<Integer, String> map = new Persistent23TreeMap<>();
        checkInsertRemove(random, map, 100);
        checkInsertRemove(random, map, ENTRIES_TO_ADD);
        for (int i = 0; i < 100; i++) {
            checkInsertRemove(random, map, 100);
        }
    }

    @SuppressWarnings({"OverlyLongMethod"})
    @Test
    public void competingWritesTest() {
        Persistent23TreeMap<Integer, String> tree = new Persistent23TreeMap<>();
        Persistent23TreeMap.MutableMap<Integer, String> write1 = tree.beginWrite();
        Persistent23TreeMap.MutableMap<Integer, String> write2 = tree.beginWrite();
        write1.put(0, "0");
        write2.remove(1);
        Assert.assertTrue(write2.endWrite());
        Assert.assertTrue(write1.endWrite());
        Persistent23TreeMap.ImmutableMap<Integer, String> read = tree.beginRead();
        Assert.assertTrue(read.containsKey(0));
        Assert.assertFalse(read.containsKey(1));
        Assert.assertFalse(read.containsKey(2));
        Assert.assertFalse(read.containsKey(3));
        Assert.assertEquals(1, read.size());

        write1.put(2, "2");
        write2.put(3, "3");
        Assert.assertTrue(write1.endWrite());
        Assert.assertFalse(write2.endWrite());
        Assert.assertTrue(read.containsKey(0));
        Assert.assertFalse(read.containsKey(1));
        Assert.assertFalse(read.containsKey(2));
        Assert.assertFalse(read.containsKey(3));
        Assert.assertEquals(1, read.size());
        read = tree.beginRead();
        Assert.assertTrue(read.containsKey(0));
        Assert.assertFalse(read.containsKey(1));
        Assert.assertTrue(read.containsKey(2));
        Assert.assertFalse(read.containsKey(3));
        Assert.assertEquals(2, read.size());

        Object root = write1.getRoot();
        write1.put(2, "2");
        Assert.assertFalse(write1.getRoot() == root);
        root = write2.getRoot();
        write2.put(2, "_2");
        Assert.assertFalse(write2.getRoot() == root);
        Assert.assertTrue(write1.endWrite());
        Assert.assertFalse(write2.endWrite());
        read = tree.beginRead();
        Assert.assertTrue(read.containsKey(0));
        Assert.assertFalse(read.containsKey(1));
        Assert.assertTrue(read.containsKey(2));
        Assert.assertFalse(read.containsKey(3));
        Assert.assertEquals(2, read.size());
    }

    @SuppressWarnings({"OverlyLongMethod"})
    @Test
    public void iterationTest() {
        Random random = new Random(8234890);
        Persistent23TreeMap<Integer, String> map = new Persistent23TreeMap<>();
        Persistent23TreeMap.MutableMap<Integer, String> write = map.beginWrite();
        int[] p = genPermutation(random);
        TreeSet<Integer> added = new TreeSet<>();
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = write.size();
            Assert.assertEquals(i, size);
            if ((size & 1023) == 0 || size < 100) {
                Iterator<Integer> iterator = added.iterator();
                for (Persistent23TreeMap.Entry<Integer, String> key : write) {
                    Assert.assertTrue(iterator.hasNext());
                    Integer next = iterator.next();
                    Assert.assertEquals(next, key.getKey());
                    Assert.assertEquals(String.valueOf(next), key.getValue());
                }
                Assert.assertFalse(iterator.hasNext());

                iterator = added.iterator();
                Iterator<Persistent23TreeMap.Entry<Integer, String>> treeItr = write.iterator();
                for (int j = 0; j < size; j++) {
                    Persistent23TreeMap.Entry<Integer, String> key = treeItr.next();
                    Assert.assertTrue(iterator.hasNext());
                    Integer next = iterator.next();
                    Assert.assertEquals(next, key.getKey());
                    Assert.assertEquals(String.valueOf(next), key.getValue());
                }
                Assert.assertFalse(iterator.hasNext());
                try {
                    treeItr.next();
                    Assert.fail();
                } catch (NoSuchElementException e) {
                }
                Assert.assertFalse(treeItr.hasNext());
            }
            write.put(p[i], String.valueOf(p[i]));
            added.add(p[i]);
        }
    }

    @SuppressWarnings({"OverlyLongMethod"})
    @Test
    public void reverseIterationTest() {
        Random random = new Random(5743);
        Persistent23TreeMap.MutableMap<Integer, String> tree = new Persistent23TreeMap<Integer, String>().beginWrite();
        int[] p = genPermutation(random);
        TreeSet<Integer> added = new TreeSet<>();
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = tree.size();
            Assert.assertEquals(i, size);
            if ((size & 1023) == 0 || size < 100) {
                Iterator<Integer> iterator = added.descendingIterator();
                for (Iterator<Persistent23TreeMap.Entry<Integer, String>> treeItr = tree.reverseIterator();
                     treeItr.hasNext(); ) {
                    Assert.assertTrue(iterator.hasNext());
                    Persistent23TreeMap.Entry<Integer, String> key = treeItr.next();
                    Integer next = iterator.next();
                    Assert.assertEquals(next, key.getKey());
                    Assert.assertEquals(String.valueOf(next), key.getValue());
                }
                Assert.assertFalse(iterator.hasNext());

                iterator = added.descendingIterator();
                Iterator<Persistent23TreeMap.Entry<Integer, String>> treeItr = tree.reverseIterator();
                for (int j = 0; j < size; j++) {
                    Persistent23TreeMap.Entry<Integer, String> key = treeItr.next();
                    Assert.assertTrue(iterator.hasNext());
                    Integer next = iterator.next();
                    Assert.assertEquals(next, key.getKey());
                    Assert.assertEquals(String.valueOf(next), key.getValue());
                }
                Assert.assertFalse(iterator.hasNext());
                try {
                    treeItr.next();
                    Assert.fail();
                } catch (NoSuchElementException e) {
                }
                Assert.assertFalse(treeItr.hasNext());
            }
            tree.put(p[i], String.valueOf(p[i]));
            added.add(p[i]);
        }
    }

    @Test
    public void tailIterationTest() {
        Random random = new Random(239786);
        Persistent23TreeMap<Integer, String> map = new Persistent23TreeMap<>();
        Persistent23TreeMap.MutableMap<Integer, String> write = map.beginWrite();
        int[] p = genPermutation(random);
        TreeSet<Integer> added = new TreeSet<>();
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = write.size();
            Assert.assertEquals(i, size);
            if ((size & 1023) == 0 || size < 100) {
                if (i > 0) {
                    checkTailIteration(write, added, map.createEntry(added.first()));
                    checkTailIteration(write, added, map.createEntry(added.first() - 1));
                    checkTailIteration(write, added, map.createEntry(added.last()));
                    checkTailIteration(write, added, map.createEntry(added.last() + 1));
                }
                checkTailIteration(write, added, map.createEntry(Integer.MAX_VALUE));
                checkTailIteration(write, added, map.createEntry(Integer.MIN_VALUE));
                for (int j = 0; j < 10; j++) {
                    checkTailIteration(write, added, map.createEntry(p[i * j / 10]));
                }
            }
            write.put(p[i], String.valueOf(p[i]));
            added.add(p[i]);
        }
    }

    @Test
    public void tailReverseIterationTest() {
        Random random = new Random(239786);
        Persistent23TreeMap<Integer, String> map = new Persistent23TreeMap<>();
        Persistent23TreeMap.MutableMap<Integer, String> write = map.beginWrite();
        int[] p = genPermutation(random);
        TreeSet<Integer> added = new TreeSet<>();
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = write.size();
            Assert.assertEquals(i, size);
            if ((size & 1023) == 0 || size < 100) {
                if (i > 0) {
                    checkTailReverseIteration(write, added, map.createEntry(added.first()));
                    checkTailReverseIteration(write, added, map.createEntry(added.first() - 1));
                    checkTailReverseIteration(write, added, map.createEntry(added.last()));
                    checkTailReverseIteration(write, added, map.createEntry(added.last() + 1));
                }
                checkTailReverseIteration(write, added, map.createEntry(Integer.MAX_VALUE));
                checkTailReverseIteration(write, added, map.createEntry(Integer.MIN_VALUE));
                for (int j = 0; j < 10; j++) {
                    checkTailReverseIteration(write, added, map.createEntry(p[i * j / 10]));
                }
            }
            write.put(p[i], String.valueOf(p[i]));
            added.add(p[i]);
        }
    }

    @Test
    public void testSize() {
        Random random = new Random(249578);
        int[] p = genPermutation(random, 10000);
        final Persistent23TreeMap<Integer, String> source = new Persistent23TreeMap<>();
        Persistent23TreeMap.MutableMap<Integer, String> tree = null;
        for (int i = 0; i < p.length; i++) {
            if ((i & 15) == 0) {
                if (i > 0) {
                    tree.endWrite();
                    Assert.assertEquals(i, source.beginRead().size());
                }
                tree = source.beginWrite();
            }
            Assert.assertEquals(i, tree.size());
            tree.put(p[i], String.valueOf(p[i]));
            Assert.assertEquals(i + 1, tree.size());
            for (int j = 0; j < 3; j++) {
                tree.put(p[random.nextInt(i + 1)], p[random.nextInt(i + 1)] + " " + i + " " + j);
                Assert.assertEquals(i + 1, tree.size());
            }
        }
        tree.endWrite();
        Assert.assertEquals(p.length, source.beginRead().size());

        p = genPermutation(random, p.length);
        tree = null;
        for (int i = 0; i < p.length; i++) {
            if ((i & 15) == 0) {
                if (i > 0) {
                    tree.endWrite();
                    Assert.assertEquals(p.length - i, source.beginRead().size());
                }
                tree = source.beginWrite();
            }
            Assert.assertEquals(p.length - i, tree.size());
            tree.remove(p[i]);
            Assert.assertEquals(p.length - i - 1, tree.size());
            for (int j = 0; j < 3; j++) {
                tree.remove(p[random.nextInt(i + 1)]);
                Assert.assertEquals(p.length - i - 1, tree.size());
            }
        }
        tree.endWrite();
        Assert.assertEquals(0, source.beginRead().size());
    }

    private static void checkTailIteration(Persistent23TreeMap.MutableMap<Integer, String> tree, SortedSet<Integer> added, Persistent23TreeMap.Entry<Integer, String> first) {
        Iterator<Integer> iterator = added.tailSet(first.getKey()).iterator();
        for (Iterator<Persistent23TreeMap.Entry<Integer, String>> treeItr = tree.tailIterator(first);
             treeItr.hasNext(); ) {
            Assert.assertTrue(iterator.hasNext());
            Persistent23TreeMap.Entry<Integer, String> entry = treeItr.next();
            Integer next = iterator.next();
            Assert.assertEquals(next, entry.getKey());
            Assert.assertEquals(String.valueOf(next), entry.getValue());
        }
        Assert.assertFalse(iterator.hasNext());
    }

    private static void checkTailReverseIteration(Persistent23TreeMap.MutableMap<Integer, String> tree, SortedSet<Integer> added, Persistent23TreeMap.Entry<Integer, String> first) {
        Iterator<Integer> iterator = ((NavigableSet<Integer>) added.tailSet(first.getKey())).descendingIterator();
        for (Iterator<Persistent23TreeMap.Entry<Integer, String>> treeItr = tree.tailReverseIterator(first);
             treeItr.hasNext(); ) {
            Assert.assertTrue(iterator.hasNext());
            Persistent23TreeMap.Entry<Integer, String> entry = treeItr.next();
            Integer next = iterator.next();
            Assert.assertEquals(next, entry.getKey());
            Assert.assertEquals(String.valueOf(next), entry.getValue());
        }
        Assert.assertFalse(iterator.hasNext());
    }

    private static void checkInsertRemove(Random random, Persistent23TreeMap<Integer, String> map, int count) {
        Persistent23TreeMap.MutableMap<Integer, String> write = map.beginWrite();
        write.testConsistency();
        addEntries(random, write, count);
        removeEntries(random, write, count);
        Assert.assertEquals(0, write.size());
        Assert.assertTrue(write.isEmpty());
        Assert.assertTrue(write.endWrite());
    }

    private static void addEntries(Random random, Persistent23TreeMap.MutableMap<Integer, String> tree, int count) {
        int[] p = genPermutation(random, count);
        for (int i = 0; i < count; i++) {
            int size = tree.size();
            Assert.assertEquals(i, size);
            int key = p[i];
            tree.put(key, key + " ");
            Assert.assertFalse(tree.isEmpty());
            tree.testConsistency();
            Assert.assertEquals(i + 1, tree.size());
            tree.put(key, String.valueOf(key));
            tree.testConsistency();
            Assert.assertEquals(i + 1, tree.size());
            for (int j = 0; j <= 10; j++) {
                int testKey = p[i * j / 10];
                Assert.assertTrue(tree.containsKey(testKey));
            }
            if (i < count - 1) {
                Assert.assertFalse(tree.containsKey(p[i + 1]));
            }
        }
    }

    private static void removeEntries(Random random, Persistent23TreeMap.MutableMap<Integer, String> tree, int count) {
        int[] p = genPermutation(random, count);
        for (int i = 0; i < count; i++) {
            int size = tree.size();
            Assert.assertEquals(count - i, size);
            Assert.assertFalse(tree.isEmpty());
            int key = p[i];
            Assert.assertEquals(String.valueOf(key), tree.remove(key));
            tree.testConsistency();
            Assert.assertNull(tree.remove(key));
            tree.testConsistency();
            for (int j = 0; j <= 10; j++) {
                int testKey = p[i * j / 10];
                Assert.assertFalse(tree.containsKey(testKey));
            }
            if (i < count - 1) {
                Assert.assertTrue(tree.containsKey(p[i + 1]));
            }
        }
    }

    private static int[] genPermutation(Random random, int size) {
        int[] p = new int[size];
        for (int i = 1; i < size; i++) {
            int j = random.nextInt(i);
            p[i] = p[j];
            p[j] = i;
        }
        return p;
    }

    private static int[] genPermutation(Random random) {
        return genPermutation(random, ENTRIES_TO_ADD);
    }
}
