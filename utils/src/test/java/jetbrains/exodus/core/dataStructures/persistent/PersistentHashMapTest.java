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

public class PersistentHashMapTest {

    private static final int ENTRIES_TO_ADD = 5000;

    @Test
    public void mutableTreeRandomInsertDeleteTest() {
        Random random = new Random(2343489);
        PersistentHashMap<Integer, String> map = new PersistentHashMap<>();
        checkInsertRemove(random, map, 100);
        checkInsertRemove(random, map, ENTRIES_TO_ADD);
        for (int i = 0; i < 100; i++) {
            checkInsertRemove(random, map, 100);
        }
    }

    @Test
    public void hashKeyCollision() {
        final PersistentHashMap<HashKey, String> map = new PersistentHashMap<>();
        PersistentHashMap<HashKey, String>.MutablePersistentHashMap w = map.beginWrite();
        HashKey first = new HashKey(1);
        w.put(first, "a");
        HashKey second = new HashKey(1);
        w.put(second, "b");
        w.endWrite();
        Assert.assertEquals(2, map.getCurrent().size());
        w = map.beginWrite();
        w.removeKey(first);
        w.endWrite();
        Assert.assertEquals(1, map.getCurrent().size());
    }

    @SuppressWarnings({"OverlyLongMethod"})
    @Test
    public void competingWritesTest() {
        PersistentHashMap<Integer, String> tree = new PersistentHashMap<>();
        PersistentHashMap<Integer, String>.MutablePersistentHashMap write1 = tree.beginWrite();
        PersistentHashMap<Integer, String>.MutablePersistentHashMap write2 = tree.beginWrite();
        write1.put(0, "0");
        write2.removeKey(1);
        Assert.assertTrue(write2.endWrite());
        Assert.assertTrue(write1.endWrite());
        PersistentHashMap<Integer, String>.ImmutablePersistentHashMap read = tree.getCurrent();
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
        read = tree.getCurrent();
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
        read = tree.getCurrent();
        Assert.assertTrue(read.containsKey(0));
        Assert.assertFalse(read.containsKey(1));
        Assert.assertTrue(read.containsKey(2));
        Assert.assertFalse(read.containsKey(3));
        Assert.assertEquals(2, read.size());
    }

    @Test
    public void testOverwrite() {
        final PersistentHashMap<Integer, String> tree = new PersistentHashMap<>();
        PersistentHashMap<Integer, String>.MutablePersistentHashMap mutable = tree.beginWrite();
        mutable.put(0, "0");
        Assert.assertTrue(mutable.endWrite());
        Assert.assertEquals("0", tree.getCurrent().get(0));
        mutable = tree.beginWrite();
        mutable.put(0, "0.0");
        Assert.assertTrue(mutable.endWrite());
        Assert.assertEquals("0.0", tree.getCurrent().get(0));
    }

    private static void checkInsertRemove(Random random, PersistentHashMap<Integer, String> map, int count) {
        PersistentHashMap<Integer, String>.MutablePersistentHashMap write = map.beginWrite();
        write.checkTip();
        addEntries(random, write, count);
        removeEntries(random, write, count);
        Assert.assertEquals(0, write.size());
        Assert.assertTrue(write.isEmpty());
        Assert.assertTrue(write.endWrite());
    }

    private static void addEntries(Random random, PersistentHashMap<Integer, String>.MutablePersistentHashMap tree, int count) {
        int[] p = genPermutation(random, count);
        for (int i = 0; i < count; i++) {
            int size = tree.size();
            Assert.assertEquals(i, size);
            int key = p[i];
            tree.put(key, key + " ");
            Assert.assertFalse(tree.isEmpty());
            tree.checkTip();
            Assert.assertEquals(i + 1, tree.size());
            tree.put(key, String.valueOf(key));
            tree.checkTip();
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

    private static void removeEntries(Random random, PersistentHashMap<Integer, String>.MutablePersistentHashMap tree, int count) {
        int[] p = genPermutation(random, count);
        for (int i = 0; i < count; i++) {
            int size = tree.size();
            Assert.assertEquals(count - i, size);
            Assert.assertFalse(tree.isEmpty());
            int key = p[i];
            Assert.assertEquals(String.valueOf(key), tree.removeKey(key));
            tree.checkTip();
            Assert.assertNull(tree.removeKey(key));
            tree.checkTip();
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

    private class HashKey {
        private final int hashCode;

        private HashKey(int hashCode) {
            this.hashCode = hashCode;
        }

        // equals isn't overriden intentionally (default is identity comparison) to emulate hash collision

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

}
