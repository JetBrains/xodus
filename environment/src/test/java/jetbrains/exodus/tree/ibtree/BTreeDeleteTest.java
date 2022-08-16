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

import jetbrains.exodus.ByteBufferComparator;
import jetbrains.exodus.log.NullLoggable;
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

        t = new ImmutableBTree(log, 1, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var random = new Random(seed);

        var entriesCount = 1;
        insertDeleteAndCheckEntries(random, entriesCount);
    }

    @Test
    public void testAddRemove4Keys() {
        final long seed = System.nanoTime();
        System.out.println("testAddRemove4Keys seed : " + seed);

        t = new ImmutableBTree(log, 2, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var random = new Random(seed);

        var entriesCount = 4;
        insertDeleteAndCheckEntries(random, entriesCount);
    }

    @Test
    public void testAddRemove64Keys() {
        final long seed = System.nanoTime();
        System.out.println("testAddRemove64Keys seed : " + seed);

        t = new ImmutableBTree(log, 3, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var random = new Random(seed);

        var entriesCount = 64;
        insertDeleteAndCheckEntries(random, entriesCount);
    }


    @Test
    public void testAddRemove1KKeys() {
        final long seed = System.nanoTime();
        System.out.println("testAddRemove1KKeys seed : " + seed);

        t = new ImmutableBTree(log, 4, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var random = new Random(seed);

        var entriesCount = 1024;
        insertDeleteAndCheckEntries(random, entriesCount);
    }

    @Test
    public void testAddRemove32KKeys() {
        final long seed = 4503391327268L;//System.nanoTime();
        System.out.println("testAddRemove32KKeys seed : " + seed);

        t = new ImmutableBTree(log, 5, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var random = new Random(seed);

        var entriesCount = 32 * 1024;
        insertDeleteAndCheckEntries(random, entriesCount);
    }

    @Test
    public void testAddRemove64KKeys() {
        final long seed = System.nanoTime();
        System.out.println("testAddRemove64KKeys seed : " + seed);

        t = new ImmutableBTree(log, 6, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var random = new Random(seed);

        var entriesCount = 64 * 1024;
        insertDeleteAndCheckEntries(random, entriesCount);
    }

    @Test
    public void testAddRemoveHalfAddHalfAgain4Keys() {
        final long seed = System.nanoTime();
        System.out.println("testAddRemoveHalfAddHalfAgain4Keys seed : " + seed);

        t = new ImmutableBTree(log, 6, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var random = new Random(seed);

        var entriesCount = 4;

        insertDeleteHalfAddHalfDeleteCheckEntries(random, entriesCount);
    }


    @Test
    public void testAddRemoveHalfAddHalfAgain64Keys() {
        final long seed = System.nanoTime();
        System.out.println("testAddRemoveHalfAddHalfAgain64Keys seed : " + seed);

        t = new ImmutableBTree(log, 7, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var random = new Random(seed);

        var entriesCount = 64;

        insertDeleteHalfAddHalfDeleteCheckEntries(random, entriesCount);
    }

    @Test
    public void testAddRemoveHalfAddHalfAgain1KKeys() {
        final long seed = System.nanoTime();
        System.out.println("testAddRemoveHalfAddHalfAgain1KKeys seed : " + seed);

        t = new ImmutableBTree(log, 8, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var random = new Random(seed);

        var entriesCount = 1024;

        insertDeleteHalfAddHalfDeleteCheckEntries(random, entriesCount);
    }

    @Test
    public void testAddRemoveHalfAddHalfAgain32KKeys() {
        final long seed = System.nanoTime();
        System.out.println("testAddRemoveHalfAddHalfAgain32KKeys seed : " + seed);

        t = new ImmutableBTree(log, 8, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var random = new Random(seed);

        var entriesCount = 32 * 1024;

        insertDeleteHalfAddHalfDeleteCheckEntries(random, entriesCount);
    }

    @Test
    public void testAddRemoveHalfAddHalfAgain64KKeys() {
        final long seed = System.nanoTime();
        System.out.println("testAddRemoveHalfAddHalfAgain64KKeys seed : " + seed);

        t = new ImmutableBTree(log, 9, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var random = new Random(seed);

        var entriesCount = 64 * 1024;

        insertDeleteHalfAddHalfDeleteCheckEntries(random, entriesCount);
    }

    private void insertDeleteAndCheckEntries(Random random, int entriesCount) {
        final TreeMap<ByteBuffer, ByteBuffer> expectedMap = new TreeMap<>(ByteBufferComparator.INSTANCE);
        var checker = new ImmutableTreeChecker(expectedMap, random);
        var interval = Math.max(1, entriesCount / 10);
        addEntries(random, entriesCount, interval, expectedMap, checker);
        removeEntries(random, expectedMap.size(), interval, expectedMap, checker);

        Assert.assertTrue(expectedMap.isEmpty());
        Assert.assertTrue(tm.isEmpty());
    }

    private void insertDeleteHalfAddHalfDeleteCheckEntries(Random random, int entriesCount) {
        final TreeMap<ByteBuffer, ByteBuffer> expectedMap = new TreeMap<>(ByteBufferComparator.INSTANCE);
        var checker = new ImmutableTreeChecker(expectedMap, random);
        var interval = Math.max(1, entriesCount / 10);

        addEntries(random, entriesCount, interval, expectedMap, checker);
        removeEntries(random, expectedMap.size() / 2, interval, expectedMap, checker);
        addEntries(random, entriesCount / 2, interval, expectedMap, checker);
        removeEntries(random, expectedMap.size(), interval, expectedMap, checker);
    }

    private void removeEntries(Random random, int entriesToRemove, int interval, TreeMap<ByteBuffer,
            ByteBuffer> expectedMap, ImmutableTreeChecker checker) {

        var iterations = Math.max(1, entriesToRemove / interval);
        if (iterations * interval < entriesToRemove) {
            iterations++;
        }

        int removed = 0;
        for (int n = 0; n < iterations; n++) {
            tm = t.getMutableCopy();

            ArrayList<ByteBuffer> keys = new ArrayList<>(expectedMap.keySet());
            Collections.shuffle(keys, random);

            var currentInterval = Math.min(entriesToRemove - removed, interval);
            for (int i = 0; i < currentInterval; i++) {
                var key = keys.get(i);
                var deleted = expectedMap.remove(key) != null;
                Assert.assertTrue(deleted);

                deleted = tm.delete(key);
                Assert.assertTrue(deleted);
                removed++;
            }

            checkAndSaveTree(false, checker);
        }
    }

    private void addEntries(Random random, int entriesToAdd, int interval, TreeMap<ByteBuffer, ByteBuffer> expectedMap,
                            ImmutableTreeChecker checker) {
        var iterations = Math.max(1, entriesToAdd / interval);
        if (iterations * interval < entriesToAdd) {
            iterations++;
        }

        int added = 0;
        for (int n = 0; n < iterations; n++) {
            var currentInterval = Math.min(entriesToAdd - added, interval);
            tm = t.getMutableCopy();

            for (int i = 0; i < currentInterval; i++) {
                var keySize = random.nextInt(1, 16);
                var keyArray = new byte[keySize];
                random.nextBytes(keyArray);

                var valueArray = new byte[32];
                random.nextBytes(valueArray);

                var key = ByteBuffer.wrap(keyArray);
                var value = ByteBuffer.wrap(valueArray);

                expectedMap.put(key, value);
                tm.put(key, value);

                added++;
            }

            checkAndSaveTree(false, checker);
        }
    }
}
