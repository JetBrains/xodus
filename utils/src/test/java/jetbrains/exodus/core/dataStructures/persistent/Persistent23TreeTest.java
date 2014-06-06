/**
 * Copyright 2010 - 2014 JetBrains s.r.o.
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
import java.util.concurrent.CyclicBarrier;

public class Persistent23TreeTest {

    static final int ENTRIES_TO_ADD = 10000;

    @Test
    public void mutableTreeRandomInsertDeleteTest() {
        Random random = new Random(2343489);
        Persistent23Tree<Integer> set = new Persistent23Tree<Integer>();
        checkInsertRemove(random, set, 100);
        checkInsertRemove(random, set, ENTRIES_TO_ADD);
        for (int i = 0; i < 100; i++) {
            checkInsertRemove(random, set, 100);
        }
    }

    @SuppressWarnings({"OverlyLongMethod"})
    @Test
    public void competingWritesTest() {
        Persistent23Tree<Integer> tree = new Persistent23Tree<Integer>();
        Persistent23Tree.MutableTree<Integer> write1 = tree.beginWrite();
        Persistent23Tree.MutableTree<Integer> write2 = tree.beginWrite();
        write1.add(0);
        write2.exclude(1);
        Assert.assertTrue(write2.endWrite());
        Assert.assertTrue(write1.endWrite());
        Persistent23Tree.ImmutableTree<Integer> read = tree.getCurrent();
        Assert.assertTrue(read.contains(0));
        Assert.assertFalse(read.contains(1));
        Assert.assertFalse(read.contains(2));
        Assert.assertFalse(read.contains(3));
        Assert.assertEquals(1, read.size());

        write1.add(2);
        write2.add(3);
        Assert.assertTrue(write1.endWrite());
        Assert.assertFalse(write2.endWrite());
        Assert.assertTrue(read.contains(0));
        Assert.assertFalse(read.contains(1));
        Assert.assertFalse(read.contains(2));
        Assert.assertFalse(read.contains(3));
        Assert.assertEquals(1, read.size());
        read = tree.getCurrent();
        Assert.assertTrue(read.contains(0));
        Assert.assertFalse(read.contains(1));
        Assert.assertTrue(read.contains(2));
        Assert.assertFalse(read.contains(3));
        Assert.assertEquals(2, read.size());

        AbstractPersistent23Tree.Node<Integer> root = write1.getRoot();
        write1.add(2);
        Assert.assertFalse(write1.getRoot() == root);
        write2.add(3);
        Assert.assertTrue(write1.endWrite());
        Assert.assertFalse(write2.endWrite());
        read = tree.getCurrent();
        Assert.assertTrue(read.contains(0));
        Assert.assertFalse(read.contains(1));
        Assert.assertTrue(read.contains(2));
        Assert.assertFalse(read.contains(3));
        Assert.assertEquals(2, read.size());
    }

    @SuppressWarnings({"OverlyLongMethod"})
    @Test
    public void iterationTest() {
        Random random = new Random(8234890);
        Persistent23Tree.MutableTree<Integer> tree = new Persistent23Tree<Integer>().beginWrite();
        int[] p = genPermutation(random);
        TreeSet<Integer> added = new TreeSet<Integer>();
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = tree.size();
            Assert.assertEquals(i, size);
            if ((size & 1023) == 0 || size < 100) {
                System.out.println(size + " added");
                Iterator<Integer> iterator = added.iterator();
                for (Integer key : tree) {
                    Assert.assertTrue(iterator.hasNext());
                    Assert.assertEquals(iterator.next(), key);
                }
                Assert.assertFalse(iterator.hasNext());
                System.out.println(size + " iterated");

                iterator = added.iterator();
                Iterator<Integer> treeItr = tree.iterator();
                for (int j = 0; j < size; j++) {
                    Integer key = treeItr.next();
                    Assert.assertTrue(iterator.hasNext());
                    Assert.assertEquals(iterator.next(), key);
                }
                Assert.assertFalse(iterator.hasNext());
                try {
                    treeItr.next();
                    Assert.fail();
                } catch (NoSuchElementException e) {
                }
                Assert.assertFalse(treeItr.hasNext());
                System.out.println(size + " iterated");
            }
            tree.add(p[i]);
            added.add(p[i]);
        }
    }

    @SuppressWarnings({"OverlyLongMethod"})
    @Test
    public void reverseIterationTest() {
        Random random = new Random(5743);
        Persistent23Tree.MutableTree<Integer> tree = new Persistent23Tree<Integer>().beginWrite();
        int[] p = genPermutation(random);
        TreeSet<Integer> added = new TreeSet<Integer>();
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = tree.size();
            Assert.assertEquals(i, size);
            if ((size & 1023) == 0 || size < 100) {
                System.out.println(size + " added");
                Iterator<Integer> iterator = added.descendingIterator();
                for (Iterator<Integer> treeItr = tree.reverseIterator();
                     treeItr.hasNext(); ) {
                    Assert.assertTrue(iterator.hasNext());
                    Integer key = treeItr.next();
                    Assert.assertEquals(iterator.next(), key);
                }
                Assert.assertFalse(iterator.hasNext());
                System.out.println(size + " iterated");

                iterator = added.descendingIterator();
                Iterator<Integer> treeItr = tree.reverseIterator();
                for (int j = 0; j < size; j++) {
                    Integer key = treeItr.next();
                    Assert.assertTrue(iterator.hasNext());
                    Assert.assertEquals(iterator.next(), key);
                }
                Assert.assertFalse(iterator.hasNext());
                try {
                    treeItr.next();
                    Assert.fail();
                } catch (NoSuchElementException e) {
                }
                Assert.assertFalse(treeItr.hasNext());
                System.out.println(size + " iterated");
            }
            tree.add(p[i]);
            added.add(p[i]);
        }
    }

    @Test
    public void tailIterationTest() {
        Random random = new Random(239786);
        Persistent23Tree.MutableTree<Integer> tree = new Persistent23Tree<Integer>().beginWrite();
        int[] p = genPermutation(random);
        TreeSet<Integer> added = new TreeSet<Integer>();
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = tree.size();
            Assert.assertEquals(i, size);
            if ((size & 1023) == 0 || size < 100) {
                System.out.println(size + " added");
                if (i > 0) {
                    checkTailIteration(tree, added, added.first());
                    checkTailIteration(tree, added, added.first() - 1);
                    checkTailIteration(tree, added, added.last());
                    checkTailIteration(tree, added, added.last() + 1);
                }
                checkTailIteration(tree, added, Integer.MAX_VALUE);
                checkTailIteration(tree, added, Integer.MIN_VALUE);
                for (int j = 0; j < 10; j++) {
                    checkTailIteration(tree, added, p[i * j / 10]);
                }
                System.out.println(size + " iterated");
            }
            tree.add(p[i]);
            added.add(p[i]);
        }
    }

    @Test
    public void tailReverseIterationTest() {
        Random random = new Random(239786);
        Persistent23Tree.MutableTree<Integer> tree = new Persistent23Tree<Integer>().beginWrite();
        int[] p = genPermutation(random);
        TreeSet<Integer> added = new TreeSet<Integer>();
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = tree.size();
            Assert.assertEquals(i, size);
            if ((size & 1023) == 0 || size < 100) {
                System.out.println(size + " added");
                if (i > 0) {
                    checkTailReverseIteration(tree, added, added.first());
                    checkTailReverseIteration(tree, added, added.first() - 1);
                    checkTailReverseIteration(tree, added, added.last());
                    checkTailReverseIteration(tree, added, added.last() + 1);
                }
                checkTailReverseIteration(tree, added, Integer.MAX_VALUE);
                checkTailReverseIteration(tree, added, Integer.MIN_VALUE);
                for (int j = 0; j < 10; j++) {
                    checkTailReverseIteration(tree, added, p[i * j / 10]);
                }
                System.out.println(size + " iterated");
            }
            tree.add(p[i]);
            added.add(p[i]);
        }
    }

    private static void checkTailIteration(Persistent23Tree.MutableTree<Integer> tree, SortedSet<Integer> added, Integer first) {
        Iterator<Integer> iterator = added.tailSet(first).iterator();
        for (Iterator<Integer> treeItr = tree.tailIterator(first);
             treeItr.hasNext(); ) {
            Assert.assertTrue(iterator.hasNext());
            Integer key = treeItr.next();
            Assert.assertEquals(iterator.next(), key);
        }
        Assert.assertFalse(iterator.hasNext());
    }

    private static void checkTailReverseIteration(Persistent23Tree.MutableTree<Integer> tree, SortedSet<Integer> added, Integer first) {
        Iterator<Integer> iterator = ((NavigableSet<Integer>) added.tailSet(first)).descendingIterator();
        for (Iterator<Integer> treeItr = tree.tailReverseIterator(first);
             treeItr.hasNext(); ) {
            Assert.assertTrue(iterator.hasNext());
            Integer key = treeItr.next();
            Assert.assertEquals(iterator.next(), key);
        }
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testAddAll() {
        final Persistent23Tree<Integer> source = new Persistent23Tree<Integer>();
        ArrayList<Integer> entries = new ArrayList<Integer>(7000000);
        for (int i = 0; i < 10000; i++) {
            final Persistent23Tree.MutableTree<Integer> tree = source.beginWrite();
            tree.addAll(entries.iterator(), i);
            Assert.assertEquals(i, tree.size());
            int j = 0;
            for (Integer key : tree) {
                Assert.assertEquals(new Integer(j), key);
                j++;
            }
            tree.checkTip();
            entries.add(i);
        }
    }

    @Test
    public void testGetMinimumMaximum() {
        final Persistent23Tree<Integer> source = new Persistent23Tree<Integer>();
        final Persistent23Tree.MutableTree<Integer> tree = source.beginWrite();
        for (int i = 0; i < 7000000; i++) {
            tree.add(i);
        }
        Assert.assertTrue(tree.endWrite());
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.out.print("Memory used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        final Persistent23Tree.ImmutableTree<Integer> current = source.getCurrent();
        Integer min = current.getMinimum();
        Assert.assertEquals(0, min.intValue());
        Integer max = tree.getMaximum();
        Assert.assertEquals(6999999, max.intValue());
    }

    @Test
    public void testGetMinimumMaximumAddAll() {
        final Persistent23Tree<Integer> source = new Persistent23Tree<Integer>();
        final Persistent23Tree.MutableTree<Integer> tree = source.beginWrite();
        tree.addAll(new Iterator<Integer>() {
            private int current = -1;

            @Override
            public boolean hasNext() {
                return current + 1 < 7000000;
            }

            @Override
            public Integer next() {
                return ++current;
            }

            @Override
            public void remove() {
            }
        }, 7000000);
        Assert.assertTrue(tree.endWrite());
        System.gc();
        System.out.print("Memory used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        final Persistent23Tree.ImmutableTree<Integer> current = source.getCurrent();
        Integer min = current.getMinimum();
        Assert.assertEquals(0, min.intValue());
        Integer max = tree.getMaximum();
        Assert.assertEquals(6999999, max.intValue());
        tree.checkTip();
    }

    @Test
    public void testGetMinimumMaximum2() {
        final Persistent23Tree<Integer> source = new Persistent23Tree<Integer>();
        final Persistent23Tree.MutableTree<Integer> tree = source.beginWrite();
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        Random rnd = new Random();
        for (int i = 0; i < 7000000; i++) {
            final Integer key = rnd.nextInt();
            tree.add(key);
            if (key > max) {
                max = key;
            }
            if (key < min) {
                min = key;
            }
        }
        Assert.assertTrue(tree.endWrite());
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.out.print("Memory used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        final Persistent23Tree.ImmutableTree<Integer> current = source.getCurrent();
        Assert.assertEquals(min, current.getMinimum().intValue());
        Assert.assertEquals(max, tree.getMaximum().intValue());
        tree.checkTip();
    }

    @Test
    public void testGetMinimumMaximum3() {
        final Persistent23Tree<Integer> source = new Persistent23Tree<Integer>();
        final Persistent23Tree.MutableTree<Integer> tree = source.beginWrite();
        for (int i = 6999999; i >= 0; --i) {
            tree.add(i);
        }
        Assert.assertTrue(tree.endWrite());
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.gc();
        System.out.print("Memory used: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        final Persistent23Tree.ImmutableTree<Integer> current = source.getCurrent();
        Integer min = current.getMinimum();
        Assert.assertEquals(0, min.intValue());
        Integer max = tree.getMaximum();
        Assert.assertEquals(6999999, max.intValue());
    }

    @Test
    public void testCache() {
        Random random = new Random(34790);
        int[] p = genPermutation(random, 7000000);
        final Persistent23Tree<Integer> source = new Persistent23Tree<Integer>();
        Persistent23Tree.MutableTree<Integer> tree = null;
        for (int i = 0; i < p.length; i++) {
            if ((i & 15) == 0) {
                if (i > 0) {
                    tree.endWrite();
                }
                tree = source.beginWrite();
            }
            tree.add(p[i]);
            if (i >= 4048) {
                tree.exclude(p[i - 4048]);
            }
        }
    }

    @Test
    public void testSize() {
        Random random = new Random(249578);
        int[] p = genPermutation(random, 10000);
        final Persistent23Tree<Integer> source = new Persistent23Tree<Integer>();
        Persistent23Tree.MutableTree<Integer> tree = null;
        for (int i = 0; i < p.length; i++) {
            if ((i & 15) == 0) {
                if (i > 0) {
                    tree.endWrite();
                    Assert.assertEquals(i, source.size());
                }
                tree = source.beginWrite();
            }
            Assert.assertEquals(i, tree.size());
            tree.add(p[i]);
            Assert.assertEquals(i + 1, tree.size());
            for (int j = 0; j < 3; j++) {
                tree.add(p[random.nextInt(i + 1)]);
                Assert.assertEquals(i + 1, tree.size());
            }
        }
        tree.endWrite();
        Assert.assertEquals(p.length, source.size());

        p = genPermutation(random, p.length);
        tree = null;
        for (int i = 0; i < p.length; i++) {
            if ((i & 15) == 0) {
                if (i > 0) {
                    tree.endWrite();
                    Assert.assertEquals(p.length - i, source.size());
                }
                tree = source.beginWrite();
            }
            Assert.assertEquals(p.length - i, tree.size());
            tree.exclude(p[i]);
            Assert.assertEquals(p.length - i - 1, tree.size());
            for (int j = 0; j < 3; j++) {
                tree.exclude(p[random.nextInt(i + 1)]);
                Assert.assertEquals(p.length - i - 1, tree.size());
            }
        }
        tree.endWrite();
        Assert.assertEquals(0, source.size());
    }

    @Test
    public void testSizeAtomicity() throws InterruptedException { // for XD-259
        final Persistent23Tree<Integer> source = new Persistent23Tree<Integer>();
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final int itr = 10000;
        final List<Throwable> errors = new LinkedList<Throwable>();
        final Thread writer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    boolean even = true;
                    for (int i = 0; i < itr; i++) {
                        final Persistent23Tree.MutableTree<Integer> tree = source.beginWrite();
                        if (even) {
                            tree.add(1);
                            tree.add(2);
                            even = false;
                        } else {
                            tree.exclude(1);
                            tree.exclude(2);
                            even = true;
                        }
                        tree.endWrite();
                    }
                } catch (Throwable t) {
                    rememberError(errors, t);
                }
            }
        });
        final Thread reader = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    for (int i = 0; i < itr; i++) {
                        final Persistent23Tree.ImmutableTree<Integer> tree = source.getCurrent();
                        int size = 0;
                        for (final Integer ignored : tree) {
                            size++;
                        }
                        Assert.assertEquals("at reader iteration " + i, size, tree.size());
                    }
                } catch (Throwable t) {
                    rememberError(errors, t);
                }
            }
        });
        writer.start();
        reader.start();
        writer.join();
        reader.join();
        for (final Throwable t : errors) {
            t.printStackTrace();
        }
        Assert.assertTrue(errors.isEmpty());
    }

    private void rememberError(List<Throwable> errors, Throwable t) {
        synchronized (errors) {
            errors.add(t);
        }
    }

    private static void checkInsertRemove(Random random, Persistent23Tree<Integer> set, int count) {
        Persistent23Tree.MutableTree<Integer> tree = set.beginWrite();
        tree.checkTip();
        addEntries(random, tree, count);
        removeEntries(random, tree, count);
        Assert.assertEquals(0, tree.size());
        Assert.assertTrue(tree.isEmpty());
        Assert.assertTrue(tree.endWrite());
    }

    private static void addEntries(Random random, Persistent23Tree.MutableTree<Integer> tree, int count) {
        int[] p = genPermutation(random, count);
        for (int i = 0; i < count; i++) {
            int size = tree.size();
            Assert.assertEquals(i, size);
            if (size > 0 && (size & 1023) == 0) {
                System.out.println(size + " added");
            }
            int key = p[i];
            tree.add(key);
            Assert.assertFalse(tree.isEmpty());
            tree.checkTip();
            tree.add(key);
            tree.checkTip();
            for (int j = 0; j <= 10; j++) {
                int testKey = p[i * j / 10];
                Assert.assertTrue(tree.contains(testKey));
            }
            if (i < count - 1) {
                Assert.assertFalse(tree.contains(p[i + 1]));
            }
        }
    }

    private static void removeEntries(Random random, Persistent23Tree.MutableTree<Integer> tree, int count) {
        int[] p = genPermutation(random, count);
        for (int i = 0; i < count; i++) {
            int size = tree.size();
            Assert.assertEquals(count - i, size);
            if ((size & 1023) == 0) {
                System.out.println(size + " left");
            }
            Assert.assertFalse(tree.isEmpty());
            int key = p[i];
            Assert.assertTrue(tree.exclude(key));
            tree.checkTip();
            Assert.assertFalse(tree.exclude(key));
            tree.checkTip();
            for (int j = 0; j <= 10; j++) {
                int testKey = p[i * j / 10];
                Assert.assertFalse(tree.contains(testKey));
            }
            if (i < count - 1) {
                Assert.assertTrue(tree.contains(p[i + 1]));
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
