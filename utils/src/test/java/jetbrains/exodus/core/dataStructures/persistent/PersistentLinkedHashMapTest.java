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

public class PersistentLinkedHashMapTest {

    private static final int ENTRIES_TO_ADD = 5000;

    @Test
    public void mutableTreeRandomInsertDeleteTest() {
        Random random = new Random(2343489);
        PersistentLinkedHashMap<Integer, String> map = new PersistentLinkedHashMap<>();
        checkInsertRemove(random, map, 100);
        checkInsertRemove(random, map, ENTRIES_TO_ADD);
        for (int i = 0; i < 100; i++) {
            checkInsertRemove(random, map, 100);
        }
    }

    @Test
    public void testOverwrite() {
        final PersistentLinkedHashMap<Integer, String> tree = new PersistentLinkedHashMap<>();
        PersistentLinkedHashMap.PersistentLinkedHashMapMutable<Integer, String> mutable = tree.beginWrite();
        mutable.put(0, "0");
        Assert.assertTrue(tree.endWrite(mutable));
        Assert.assertEquals("0", tree.beginWrite().get(0));
        mutable = tree.beginWrite();
        mutable.put(0, "0.0");
        Assert.assertTrue(tree.endWrite(mutable));
        Assert.assertEquals("0.0", tree.beginWrite().get(0));
    }

    private static void checkInsertRemove(Random random, PersistentLinkedHashMap<Integer, String> map, int count) {
        final PersistentLinkedHashMap.PersistentLinkedHashMapMutable<Integer, String> write = map.beginWrite();
        write.checkTip();
        addEntries(random, write, count);
        removeEntries(random, write, count);
        Assert.assertEquals(0, write.size());
        Assert.assertTrue(write.isEmpty());
        Assert.assertTrue(map.endWrite(write));
    }

    private static void addEntries(Random random, PersistentLinkedHashMap.PersistentLinkedHashMapMutable<Integer, String> tree, int count) {
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

    private static void removeEntries(Random random, PersistentLinkedHashMap.PersistentLinkedHashMapMutable<Integer, String> tree, int count) {
        int[] p = genPermutation(random, count);
        for (int i = 0; i < count; i++) {
            int size = tree.size();
            Assert.assertEquals(count - i, size);
            Assert.assertFalse(tree.isEmpty());
            int key = p[i];
            Assert.assertEquals(String.valueOf(key), tree.remove(key));
            tree.checkTip();
            Assert.assertNull(tree.remove(key));
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
}
