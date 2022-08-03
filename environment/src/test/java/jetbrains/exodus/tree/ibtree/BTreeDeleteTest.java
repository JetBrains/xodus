/*
 * *
 *  * Copyright 2010 - 2022 JetBrains s.r.o.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package jetbrains.exodus.tree.ibtree;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteBufferComparator;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.TreeMap;

public class BTreeDeleteTest extends BTreeTestBase {
    @Test
    public void testAddRemoveSingleKey() {
        final long seed = System.nanoTime();
        System.out.println("testAddRemoveSingleKey seed : " + seed);

        createMutableTree(false, 1);
        var random = new Random(seed);

        var entriesCount = 1;
        insertDeleteAndCheckEntries(random, entriesCount);
    }

    @Test
    public void testAddRemove4Keys() {
        final long seed = System.nanoTime();
        System.out.println("testAddRemove4Keys seed : " + seed);

        createMutableTree(false, 2);
        var random = new Random(seed);

        var entriesCount = 4;
        insertDeleteAndCheckEntries(random, entriesCount);
    }

    @Test
    public void testAddRemove64Keys() {
        final long seed = System.nanoTime();
        System.out.println("testAddRemove64Keys seed : " + seed);

        createMutableTree(false, 3);
        var random = new Random(seed);

        var entriesCount = 64;
        insertDeleteAndCheckEntries(random, entriesCount);
    }


    @Test
    public void testAddRemove1KKeys() {
        final long seed = System.nanoTime();
        System.out.println("testAddRemove1KKeys seed : " + seed);

        createMutableTree(false, 4);
        var random = new Random(seed);

        var entriesCount = 1024;
        insertDeleteAndCheckEntries(random, entriesCount);
    }

    @Test
    public void testAddRemove32KKeys() {
        final long seed = System.nanoTime();
        System.out.println("testAddRemove32KKeys seed : " + seed);

        createMutableTree(false, 5);
        var random = new Random(seed);

        var entriesCount = 32 * 1024;
        insertDeleteAndCheckEntries(random, entriesCount);
    }


    private void insertDeleteAndCheckEntries(Random random, int entriesCount) {
        final TreeMap<ByteBuffer, ByteBuffer> expectedMap = new TreeMap<>(ByteBufferComparator.INSTANCE);
        for (int i = 0; i < entriesCount; i++) {
            var keySize = random.nextInt(1, 16);
            var key = new byte[keySize];
            random.nextBytes(key);

            var value = new byte[32];
            random.nextBytes(value);

            expectedMap.put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
            tm.put(new ArrayByteIterable(key), new ArrayByteIterable(value));
        }

        var checker = new ImmutableTreeChecker(expectedMap, random);
        final int border = Math.max(expectedMap.size() / 10, 1);

        while (!expectedMap.isEmpty()) {
            ArrayList<ByteBuffer> keys = new ArrayList<>(expectedMap.keySet());
            Collections.shuffle(keys, random);

            var max = Math.min(border, keys.size());

            for (int i = 0; i < max; i++) {
                var key = keys.get(i);
                var deleted = expectedMap.remove(key) != null;
                Assert.assertTrue(deleted);

                deleted = tm.delete(key);
                Assert.assertTrue(deleted);
            }

            checkTree(false, checker);

            tm = t.getMutableCopy();
        }
    }
}
