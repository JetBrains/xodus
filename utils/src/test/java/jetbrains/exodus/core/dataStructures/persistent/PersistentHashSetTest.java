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

import jetbrains.exodus.core.dataStructures.hash.ObjectProcedure;
import jetbrains.exodus.util.Random;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class PersistentHashSetTest {

    private static final int ENTRIES_TO_ADD = 5000;

    @SuppressWarnings({"OverlyLongMethod"})
    @Test
    public void mutableSetRandomInsertDeleteTest() {
        Random random = new Random(2343489);
        PersistentHashSet.MutablePersistentHashSet<Integer> tree = new PersistentHashSet<Integer>().beginWrite();
        tree.checkTip();
        int[] p = genPermutation(random);
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = tree.size();
            Assert.assertEquals(i, size);
            int key = p[i];
            tree.add(key);
            AbstractPersistentHashSet.TableNode<Integer> root = tree.getRoot();
            Assert.assertFalse(tree.isEmpty());
            tree.checkTip();
            tree.add(key);
            Assert.assertNotSame(root, tree.getRoot());
            tree.checkTip();
            for (int j = 0; j <= 10; j++) {
                int testKey = p[i * j / 10];
                Assert.assertTrue(tree.contains(testKey));
                if (i < ENTRIES_TO_ADD - 1) {
                    Assert.assertFalse(tree.contains(p[i + 1]));
                }
            }
        }
        p = genPermutation(random);
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = tree.size();
            Assert.assertEquals(ENTRIES_TO_ADD - i, size);
            Assert.assertFalse(tree.isEmpty());
            int key = p[i];
            Assert.assertTrue(tree.remove(key));
            tree.checkTip();
            Assert.assertFalse(tree.remove(key));
            tree.checkTip();
            for (int j = 0; j <= 10; j++) {
                int testKey = p[i * j / 10];
                Assert.assertFalse(tree.contains(testKey));
                if (i < ENTRIES_TO_ADD - 1) {
                    Assert.assertTrue(tree.contains(p[i + 1]));
                }
            }
        }
        Assert.assertEquals(0, tree.size());
        Assert.assertTrue(tree.isEmpty());
        Assert.assertTrue(tree.endWrite());
    }

    @Test
    public void competingWritesTest() {
        PersistentHashSet<Integer> tree = new PersistentHashSet<>();
        PersistentHashSet.MutablePersistentHashSet<Integer> write1 = tree.beginWrite();
        PersistentHashSet.MutablePersistentHashSet<Integer> write2 = tree.beginWrite();
        write1.add(0);
        write2.remove(1);
        Assert.assertTrue(write2.endWrite());
        Assert.assertTrue(write1.endWrite());
        PersistentHashSet.ImmutablePersistentHashSet<Integer> read = tree.beginRead();
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
        read = tree.beginRead();
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
        PersistentHashSet.MutablePersistentHashSet<Integer> tree = new PersistentHashSet<Integer>().beginWrite();
        int[] p = genPermutation(random);
        HashSet<Integer> added = new HashSet<>();
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = tree.size();
            Assert.assertEquals(i, size);
            if ((size & 1023) == 0 || size < 100) {
                Collection<Integer> actual = new HashSet<>(size);
                for (Integer key : tree) {
                    Assert.assertFalse(actual.contains(key));
                    actual.add(key);
                }
                Assert.assertEquals(size, actual.size());
                for (Integer key : added) {
                    Assert.assertTrue(actual.contains(key));
                }

                Iterator<Integer> treeItr = tree.iterator();
                actual.clear();
                for (int j = 0; j < size; j++) {
                    Integer key = treeItr.next();
                    Assert.assertFalse(actual.contains(key));
                    actual.add(key);
                }
                Assert.assertEquals(size, actual.size());
                for (Integer key : added) {
                    Assert.assertTrue(actual.contains(key));
                }
            }
            tree.add(p[i]);
            added.add(p[i]);
        }
    }

    @SuppressWarnings({"OverlyLongMethod"})
    @Test
    public void forEachKeyTest() {
        Random random = new Random(8234890);
        PersistentHashSet.MutablePersistentHashSet<Integer> tree = new PersistentHashSet<Integer>().beginWrite();
        int[] p = genPermutation(random);
        HashSet<Integer> added = new HashSet<>();
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = tree.size();
            Assert.assertEquals(i, size);
            if ((size & 1023) == 0 || size < 100) {
                final Collection<Integer> actual = new HashSet<>(size);
                ObjectProcedure<Integer> proc = new ObjectProcedure<Integer>() {
                    @Override
                    public boolean execute(Integer object) {
                        Assert.assertFalse(actual.contains(object));
                        actual.add(object);
                        return true;
                    }
                };
                tree.forEachKey(proc);
                Assert.assertEquals(size, actual.size());
                for (Integer key : added) {
                    Assert.assertTrue(actual.contains(key));
                }
            }
            tree.add(p[i]);
            added.add(p[i]);
        }
    }

    @Test
    public void testRootCollision() {
        final PersistentHashSet<Object> source = new PersistentHashSet<>();
        PersistentHashSet.MutablePersistentHashSet<Object> writeable = source.beginWrite();
        writeable.add(createClashingHashCodeObject());
        writeable.add(createClashingHashCodeObject());
        Assert.assertEquals(2, writeable.size());
        writeable.endWrite();
    }

    @SuppressWarnings("EqualsAndHashcode")
    private static Object createClashingHashCodeObject() {
        return new Object() {
            @Override
            public int hashCode() {
                return 0xFFFFF;
            }
        };
    }

    @Test
    public void testSizeAtomicity() throws InterruptedException {
        final PersistentHashSet<Integer> source = new PersistentHashSet<>();
        final CountDownLatch latch = new CountDownLatch(2);
        final int itr = 10000;
        final List<Throwable> errors = new LinkedList<>();
        final Thread writer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    latch.countDown();
                    boolean even = true;
                    for (int i = 0; i < itr; i++) {
                        final PersistentHashSet.MutablePersistentHashSet<Integer> tree = source.beginWrite();
                        if (even) {
                            tree.add(1);
                            tree.add(2);
                            even = false;
                        } else {
                            tree.remove(1);
                            tree.remove(2);
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
                    latch.countDown();
                    for (int i = 0; i < itr; i++) {
                        final PersistentHashSet.ImmutablePersistentHashSet<Integer> tree = source.beginRead();
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

    @Test
    public void getSavesObject() {
        final PersistentHashSet<Integer> set = new PersistentHashSet<>();
        final PersistentHashSet.MutablePersistentHashSet<Integer> mutableSet = set.beginWrite();
        @SuppressWarnings("UnnecessaryBoxing") final Integer e = new Integer(271828);
        mutableSet.add(e);
        mutableSet.endWrite();
        //noinspection NumberEquality
        Assert.assertTrue(e == set.beginRead().getKey(new Integer(271828)));
    }

    private static int[] genPermutation(Random random) {
        int[] p = new int[ENTRIES_TO_ADD];
        for (int i = 1; i < ENTRIES_TO_ADD; i++) {
            int j = random.nextInt(i);
            p[i] = p[j];
            p[j] = i;
        }
        return p;
    }
}
