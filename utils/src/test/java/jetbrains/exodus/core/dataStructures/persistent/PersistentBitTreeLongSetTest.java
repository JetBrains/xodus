/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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

import jetbrains.exodus.core.dataStructures.hash.LongIterator;
import jetbrains.exodus.util.Random;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.TreeSet;

public class PersistentBitTreeLongSetTest {

    private static final int ENTRIES_TO_ADD = 5000;

    @Test
    public void testEmpty() {
        PersistentLongSet set = createSet();
        Assert.assertFalse(set.beginRead().contains(0));
        Assert.assertFalse(set.beginRead().contains(-1));
        Assert.assertFalse(set.beginRead().contains(-2));
        Assert.assertFalse(set.beginRead().contains(3));
        Assert.assertEquals(0, set.beginRead().size());
        Assert.assertFalse(set.beginRead().longIterator().hasNext());
    }

    @Test
    public void mutableTreeRandomInsertDeleteTest() {
        Random random = new Random(2343489);
        PersistentLongSet set = createSet();
        checkInsertRemove(random, set, 100);
        checkInsertRemove(random, set, ENTRIES_TO_ADD);
        for (int i = 0; i < 100; i++) {
            checkInsertRemove(random, set, 100);
        }
    }

    @SuppressWarnings({"OverlyLongMethod"})
    @Test
    public void competingWritesTest() {
        PersistentLongSet set = createSet();
        PersistentLongSet.MutableSet write1 = set.beginWrite();
        PersistentLongSet.MutableSet write2 = set.beginWrite();
        write1.add(0);
        write2.remove(1);
        Assert.assertTrue(write2.endWrite());
        Assert.assertTrue(write1.endWrite());
        PersistentLongSet.ImmutableSet read = set.beginRead();
        Assert.assertTrue(read.contains(0));
        Assert.assertFalse(read.contains(1));
        Assert.assertFalse(read.contains(-2));
        Assert.assertFalse(read.contains(3));
        Assert.assertEquals(1, read.size());

        write1.add(-2);
        write2.add(3);
        Assert.assertTrue(write1.endWrite());
        Assert.assertFalse(write2.endWrite());
        Assert.assertTrue(read.contains(0));
        Assert.assertFalse(read.contains(1));
        Assert.assertFalse(read.contains(-2));
        Assert.assertFalse(read.contains(3));
        Assert.assertEquals(1, read.size());
        read = set.beginRead();
        Assert.assertTrue(read.contains(0));
        Assert.assertFalse(read.contains(1));
        Assert.assertTrue(read.contains(-2));
        Assert.assertFalse(read.contains(3));
        Assert.assertEquals(2, read.size());

        Object root = ((PersistentBitTreeLongSet.MutableSet) write1).getRoot();
        write1.add(3);
        Assert.assertFalse(((PersistentBitTreeLongSet.MutableSet) write1).getRoot() == root);
        root = ((PersistentBitTreeLongSet.MutableSet) write2).getRoot();
        write2.add(-2);
        Assert.assertFalse(((PersistentBitTreeLongSet.MutableSet) write2).getRoot() == root);
        Assert.assertTrue(write1.endWrite());
        Assert.assertFalse(write2.endWrite());
        read = set.beginRead();
        Assert.assertTrue(read.contains(0));
        Assert.assertFalse(read.contains(1));
        Assert.assertTrue(read.contains(-2));
        Assert.assertFalse(read.contains(4));
        Assert.assertEquals(3, read.size());
    }

    @SuppressWarnings({"OverlyLongMethod"})
    @Test
    public void iterationTest() {
        Random random = new Random(8234890);
        PersistentLongSet set = createSet();
        PersistentLongSet.MutableSet write = set.beginWrite();
        long[] p = genPermutation(random);
        TreeSet<Long> added = new TreeSet<>();
        for (int i = 0; i < ENTRIES_TO_ADD; i++) {
            int size = write.size();
            Assert.assertEquals(i, size);
            if ((size & 1023) == 0 || size < 100) {
                Iterator<Long> iterator = added.iterator();
                LongIterator setItr = write.longIterator();
                while (setItr.hasNext()) {
                    Assert.assertTrue(iterator.hasNext());
                    Long next = iterator.next();
                    Assert.assertEquals(next, setItr.next());
                }
                Assert.assertFalse(iterator.hasNext());

                iterator = added.iterator();
                setItr = write.longIterator();
                for (int j = 0; j < size; j++) {
                    Long key = setItr.next();
                    Assert.assertTrue(iterator.hasNext());
                    Long next = iterator.next();
                    Assert.assertEquals(next, key);
                }
                Assert.assertFalse(iterator.hasNext());
                try {
                    setItr.next();
                    Assert.fail();
                } catch (NoSuchElementException e) {
                }
                Assert.assertFalse(setItr.hasNext());
            }
            write.add(p[i]);
            added.add(p[i]);
        }
    }

    @Test
    public void testSize() {
        Random random = new Random(249578);
        long[] p = genPermutation(random, ENTRIES_TO_ADD);
        final PersistentLongSet source = createSet();
        PersistentLongSet.MutableSet set = null;
        for (int i = 0; i < p.length; i++) {
            if ((i & 15) == 0) {
                if (i > 0) {
                    set.endWrite();
                    Assert.assertEquals(i, source.beginRead().size());
                }
                set = source.beginWrite();
            }
            Assert.assertEquals(i, set.size());
            set.add(p[i]);
            Assert.assertEquals(i + 1, set.size());
            for (int j = 0; j < 3; j++) {
                set.add(p[random.nextInt(i + 1)]);
                Assert.assertEquals(i + 1, set.size());
            }
        }
        set.endWrite();
        Assert.assertEquals(p.length, source.beginRead().size());

        p = genPermutation(random, p.length);
        set = null;
        for (int i = 0; i < p.length; i++) {
            if ((i & 15) == 0) {
                if (i > 0) {
                    set.endWrite();
                    Assert.assertEquals(p.length - i, source.beginRead().size());
                }
                set = source.beginWrite();
            }
            Assert.assertEquals(p.length - i, set.size());
            set.remove(p[i]);
            Assert.assertEquals(p.length - i - 1, set.size());
            for (int j = 0; j < 3; j++) {
                set.remove(p[random.nextInt(i + 1)]);
                Assert.assertEquals(p.length - i - 1, set.size());
            }
        }
        set.endWrite();
        Assert.assertEquals(0, source.beginRead().size());
    }

    @NotNull
    protected PersistentLongSet createSet() {
        return new PersistentBitTreeLongSet();
    }

    private static void checkInsertRemove(Random random, PersistentLongSet set, int count) {
        PersistentLongSet.MutableSet write = set.beginWrite();
        addEntries(random, write, count);
        removeEntries(random, write, count);
        Assert.assertEquals(0, write.size());
        Assert.assertTrue(write.isEmpty());
        Assert.assertTrue(write.endWrite());
    }

    private static void addEntries(Random random, PersistentLongSet.MutableSet set, int count) {
        long[] p = genPermutation(random, count);
        for (int i = 0; i < count; i++) {
            int size = set.size();
            Assert.assertEquals(i, size);
            long key = p[i];
            set.add(key);
            Assert.assertFalse(set.isEmpty());
            Assert.assertEquals(i + 1, set.size());
            set.add(key);
            Assert.assertEquals(i + 1, set.size());
            for (int j = 0; j <= 10; j++) {
                long testKey = p[i * j / 10];
                Assert.assertTrue(set.contains(testKey));
            }
            if (i < count - 1) {
                Assert.assertFalse(set.contains(p[i + 1]));
            }
        }
    }

    private static void removeEntries(Random random, PersistentLongSet.MutableSet set, int count) {
        long[] p = genPermutation(random, count);
        for (int i = 0; i < count; i++) {
            int size = set.size();
            Assert.assertEquals(count - i, size);
            Assert.assertFalse(set.isEmpty());
            long key = p[i];
            Assert.assertTrue(set.remove(key));
            Assert.assertFalse(set.remove(key));
            for (int j = 0; j <= 10; j++) {
                long testKey = p[i * j / 10];
                Assert.assertFalse(set.contains(testKey));
            }
            if (i < count - 1) {
                Assert.assertTrue(set.contains(p[i + 1]));
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
