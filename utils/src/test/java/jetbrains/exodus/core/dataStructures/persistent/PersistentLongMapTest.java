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
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class PersistentLongMapTest {

    private static final int ENTRIES_TO_ADD = 5000;

    @Test
    public void mutableTreeRandomInsertDeleteTest() {
        Random random = new Random(2343489);
        PersistentLongMap<String> map = createMap();
        checkInsertRemove(random, map, 100);
        checkInsertRemove(random, map, ENTRIES_TO_ADD);
        for (int i = 0; i < 100; i++) {
            checkInsertRemove(random, map, 100);
        }
    }

    @SuppressWarnings({"OverlyLongMethod"})
    @Test
    public void competingWritesTest() {
        PersistentLongMap<String> tree = createMap();
        PersistentLongMap.MutableMap<String> write1 = tree.beginWrite();
        PersistentLongMap.MutableMap<String> write2 = tree.beginWrite();
        write1.put(0, "0");
        write2.remove(1);
        Assert.assertTrue(write2.endWrite());
        Assert.assertTrue(write1.endWrite());
        PersistentLongMap.ImmutableMap<String> read = tree.beginRead();
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

        Object root = ((RootHolder) write1).getRoot();
        write1.put(2, "2");
        Assert.assertFalse(((RootHolder) write1).getRoot() == root);
        root = ((RootHolder) write2).getRoot();
        write2.put(2, "_2");
        Assert.assertFalse(((RootHolder) write2).getRoot() == root);
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
        PersistentLongMap<String> map = createMap();
        PersistentLongMap.MutableMap<String> write = map.beginWrite();
        long[] p = genPermutation(random);
        TreeSet<Long> added = new TreeSet<>();
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = write.size();
            Assert.assertEquals(i, size);
            if ((size & 1023) == 0 || size < 100) {
                Iterator<Long> iterator = added.iterator();
                for (PersistentLongMap.Entry<String> entry : write) {
                    Assert.assertTrue(iterator.hasNext());
                    long next = iterator.next();
                    Assert.assertEquals(next, entry.getKey());
                    Assert.assertEquals(String.valueOf(next), entry.getValue());
                }
                Assert.assertFalse(iterator.hasNext());

                boolean first = true;
                iterator = added.iterator();
                Iterator<PersistentLongMap.Entry<String>> treeItr = write.iterator();
                for (int j = 0; j < size; j++) {
                    PersistentLongMap.Entry<String> key = treeItr.next();
                    Assert.assertTrue(iterator.hasNext());
                    long next = iterator.next();
                    if (first) {
                        Assert.assertEquals(new LongMapEntry<>(next, String.valueOf(next)), write.getMinimum());
                        first = false;
                    }
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
        PersistentLongMap.MutableMap<String> tree = createMap().beginWrite();
        long[] p = genPermutation(random);
        TreeSet<Long> added = new TreeSet<>();
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = tree.size();
            Assert.assertEquals(i, size);
            if ((size & 1023) == 0 || size < 100) {
                Iterator<Long> iterator = added.descendingIterator();
                for (Iterator<PersistentLongMap.Entry<String>> treeItr = tree.reverseIterator();
                     treeItr.hasNext(); ) {
                    Assert.assertTrue(iterator.hasNext());
                    PersistentLongMap.Entry<String> key = treeItr.next();
                    long next = iterator.next();
                    Assert.assertEquals(next, key.getKey());
                    Assert.assertEquals(String.valueOf(next), key.getValue());
                }
                Assert.assertFalse(iterator.hasNext());

                iterator = added.descendingIterator();
                Iterator<PersistentLongMap.Entry<String>> treeItr = tree.reverseIterator();
                for (int j = 0; j < size; j++) {
                    PersistentLongMap.Entry<String> key = treeItr.next();
                    Assert.assertTrue(iterator.hasNext());
                    long next = iterator.next();
                    Assert.assertEquals(next, key.getKey());
                    Assert.assertEquals(String.valueOf(next), key.getValue());
                }
                Assert.assertFalse(iterator.hasNext());
                try {
                    treeItr.next();
                    Assert.fail();
                } catch (NoSuchElementException ignored) {
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
        PersistentLongMap<String> map = createMap();
        PersistentLongMap.MutableMap<String> write = map.beginWrite();
        long[] p = genPermutation(random);
        TreeSet<Long> added = new TreeSet<>();
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = write.size();
            Assert.assertEquals(i, size);
            if ((size & 1023) == 0 || size < 100) {
                if (i > 0) {
                    checkTailIteration(write, added, added.first());
                    checkTailIteration(write, added, added.first() - 1);
                    checkTailIteration(write, added, added.last());
                    checkTailIteration(write, added, added.last() + 1);
                }
                checkTailIteration(write, added, Long.MAX_VALUE);
                checkTailIteration(write, added, Long.MIN_VALUE);
                for (int j = 0; j < 10; j++) {
                    checkTailIteration(write, added, p[i * j / 10]);
                }
            }
            write.put(p[i], String.valueOf(p[i]));
            added.add(p[i]);
        }
    }

    @Test
    public void tailReverseIterationTest() {
        Random random = new Random(239786);
        PersistentLongMap<String> map = createMap();
        PersistentLongMap.MutableMap<String> write = map.beginWrite();
        long[] p = genPermutation(random);
        TreeSet<Long> added = new TreeSet<>();
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = write.size();
            Assert.assertEquals(i, size);
            if ((size & 1023) == 0 || size < 100) {
                if (i > 0) {
                    checkTailReverseIteration(write, added, added.first());
                    checkTailReverseIteration(write, added, added.first() - 1);
                    checkTailReverseIteration(write, added, added.last());
                    checkTailReverseIteration(write, added, added.last() + 1);
                }
                checkTailReverseIteration(write, added, Long.MAX_VALUE);
                checkTailReverseIteration(write, added, Long.MIN_VALUE);
                for (int j = 0; j < 10; j++) {
                    checkTailReverseIteration(write, added, p[i * j / 10]);
                }
            }
            write.put(p[i], String.valueOf(p[i]));
            added.add(p[i]);
        }
    }

    @Test
    public void testSize() {
        Random random = new Random(249578);
        long[] p = genPermutation(random, ENTRIES_TO_ADD);
        final PersistentLongMap<String> source = createMap();
        PersistentLongMap.MutableMap<String> tree = null;
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

    @NotNull
    protected PersistentLongMap<String> createMap() {
        return new PersistentLong23TreeMap<>();
    }

    private static void checkTailIteration(PersistentLongMap.MutableMap<String> tree, SortedSet<Long> added, long first) {
        Iterator<Long> iterator = added.tailSet(first).iterator();
        for (Iterator<PersistentLongMap.Entry<String>> treeItr = tree.tailEntryIterator(first);
             treeItr.hasNext(); ) {
            Assert.assertTrue(iterator.hasNext());
            PersistentLongMap.Entry<String> entry = treeItr.next();
            long next = iterator.next();
            Assert.assertEquals(next, entry.getKey());
            Assert.assertEquals(String.valueOf(next), entry.getValue());
        }
        Assert.assertFalse(iterator.hasNext());
    }

    private static void checkTailReverseIteration(PersistentLongMap.MutableMap<String> tree, SortedSet<Long> added, long first) {
        Iterator<Long> iterator = ((NavigableSet<Long>) added.tailSet(first)).descendingIterator();
        for (Iterator<PersistentLongMap.Entry<String>> treeItr = tree.tailReverseEntryIterator(first);
             treeItr.hasNext(); ) {
            Assert.assertTrue(iterator.hasNext());
            PersistentLongMap.Entry<String> entry = treeItr.next();
            long next = iterator.next();
            Assert.assertEquals(next, entry.getKey());
            Assert.assertEquals(String.valueOf(next), entry.getValue());
        }
        Assert.assertFalse(iterator.hasNext());
    }

    private static void checkInsertRemove(Random random, PersistentLongMap<String> map, int count) {
        PersistentLongMap.MutableMap<String> write = map.beginWrite();
        write.testConsistency();
        addEntries(random, write, count);
        removeEntries(random, write, count);
        Assert.assertEquals(0, write.size());
        Assert.assertTrue(write.isEmpty());
        Assert.assertTrue(write.endWrite());
    }

    private static void addEntries(Random random, PersistentLongMap.MutableMap<String> tree, int count) {
        long[] p = genPermutation(random, count);
        for (int i = 0; i < count; i++) {
            int size = tree.size();
            Assert.assertEquals(i, size);
            long key = p[i];
            tree.put(key, key + " ");
            Assert.assertFalse(tree.isEmpty());
            tree.testConsistency();
            Assert.assertEquals(i + 1, tree.size());
            tree.put(key, String.valueOf(key));
            tree.testConsistency();
            Assert.assertEquals(i + 1, tree.size());
            for (int j = 0; j <= 10; j++) {
                long testKey = p[i * j / 10];
                Assert.assertTrue(tree.containsKey(testKey));
            }
            if (i < count - 1) {
                Assert.assertFalse(tree.containsKey(p[i + 1]));
            }
        }
    }

    private static void removeEntries(Random random, PersistentLongMap.MutableMap<String> tree, int count) {
        long[] p = genPermutation(random, count);
        for (int i = 0; i < count; i++) {
            int size = tree.size();
            Assert.assertEquals(count - i, size);
            Assert.assertFalse(tree.isEmpty());
            long key = p[i];
            Assert.assertEquals(String.valueOf(key), tree.remove(key));
            tree.testConsistency();
            Assert.assertNull(tree.remove(key));
            tree.testConsistency();
            for (int j = 0; j <= 10; j++) {
                long testKey = p[i * j / 10];
                Assert.assertFalse(tree.containsKey(testKey));
            }
            if (i < count - 1) {
                Assert.assertTrue(tree.containsKey(p[i + 1]));
            }
        }
    }

    private static long[] genPermutation(Random random, int size) {
        long[] p = new long[size];
        for (int i = 1; i < size; i++) {
            int j = random.nextInt(i);
            p[i] = p[j];
            p[j] = i;
        }
        return p;
    }

    private static long[] genPermutation(Random random) {
        return genPermutation(random, ENTRIES_TO_ADD);
    }
}
