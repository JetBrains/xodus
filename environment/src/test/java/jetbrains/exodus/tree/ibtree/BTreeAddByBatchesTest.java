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
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.TreeMap;

public class BTreeAddByBatchesTest extends BTreeTestBase {
    @Test
    public void testAdd32KByBatchOf1() {
        final long seed = System.nanoTime();
        System.out.println("testAdd32KByBatchOf1 seed : " + seed);

        t = new ImmutableBTree(log, 1, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 32 * 1024;
        var batchSize = 1;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd32KByBatchOf4() {
        final long seed = System.nanoTime();
        System.out.println("testAdd32KByBatchOf4 seed : " + seed);

        t = new ImmutableBTree(log, 2, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 32 * 1024;
        var batchSize = 4;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd32KByBatchOf8() {
        final long seed = System.nanoTime();
        System.out.println("testAdd32KByBatchOf8 seed : " + seed);

        t = new ImmutableBTree(log, 3, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 32 * 1024;
        var batchSize = 8;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd32KByBatchOf64() {
        final long seed = System.nanoTime();
        System.out.println("testAdd32KByBatchOf64 seed : " + seed);

        t = new ImmutableBTree(log, 4, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 32 * 1024;
        var batchSize = 64;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd32KByBatchOf256() {
        final long seed = System.nanoTime();
        System.out.println("testAdd32KByBatchOf256 seed : " + seed);

        t = new ImmutableBTree(log, 5, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 32 * 1024;
        var batchSize = 256;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd32KByBatchOf1024() {
        final long seed = System.nanoTime();
        System.out.println("testAdd32KByBatchOf1024 seed : " + seed);

        t = new ImmutableBTree(log, 6, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 32 * 1024;
        var batchSize = 1024;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd32KByBatchOf4096() {
        final long seed = System.nanoTime();
        System.out.println("testAdd32KByBatchOf4096 seed : " + seed);

        t = new ImmutableBTree(log, 7, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 32 * 1024;
        var batchSize = 4096;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd64KByBatchOf1() {
        final long seed = System.nanoTime();
        System.out.println("testAdd64KByBatchOf1 seed : " + seed);

        t = new ImmutableBTree(log, 8, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 64 * 1024;
        var batchSize = 1;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }


    @Test
    public void testAdd64KByBatchOf4() {
        final long seed = System.nanoTime();
        System.out.println("testAdd64KByBatchOf4 seed : " + seed);

        t = new ImmutableBTree(log, 9, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 64 * 1024;
        var batchSize = 4;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd64KByBatchOf8() {
        final long seed = System.nanoTime();
        System.out.println("testAdd64KByBatchOf8 seed : " + seed);

        t = new ImmutableBTree(log, 10, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 64 * 1024;
        var batchSize = 8;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd64KByBatchOf64() {
        final long seed = System.nanoTime();
        System.out.println("testAdd64KByBatchOf64 seed : " + seed);

        t = new ImmutableBTree(log, 11, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 64 * 1024;
        var batchSize = 64;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd64KByBatchOf256() {
        final long seed = System.nanoTime();
        System.out.println("testAdd64KByBatchOf256 seed : " + seed);

        t = new ImmutableBTree(log, 12, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 64 * 1024;
        var batchSize = 256;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd64KByBatchOf1024() {
        final long seed = System.nanoTime();
        System.out.println("testAdd64KByBatchOf1024 seed : " + seed);

        t = new ImmutableBTree(log, 13, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 64 * 1024;
        var batchSize = 1024;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd64KByBatchOf4096() {
        final long seed = System.nanoTime();
        System.out.println("testAdd64KByBatchOf4096 seed : " + seed);

        t = new ImmutableBTree(log, 14, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 64 * 1024;
        var batchSize = 4096;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }


    private void addKeysByBatches(int entriesToAdd, int batchSize, int checkInterval, Random random) {
        final TreeMap<ByteBuffer, ByteBuffer> expectedMap = new TreeMap<>(ByteBufferComparator.INSTANCE);
        var checker = new ImmutableTreeChecker(expectedMap, random);

        int added = 0;
        int checkIteration = 0;

        int iterations = entriesToAdd / batchSize;
        if (iterations * batchSize < entriesToAdd) {
            iterations++;
        }

        tm = t.getMutableCopy();
        for (int n = 0; n < iterations; n++) {
            var currentBatchSize = Math.min(entriesToAdd - added, batchSize);

            for (int i = 0; i < currentBatchSize; i++) {
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

                if (added / checkInterval != checkIteration) {
                    checker.accept(tm);
                    checkIteration = added / checkInterval;
                }
            }

            var structureId = t.getStructureId();
            long address = saveTree();
            t = openTree(address, false, structureId);
            tm = t.getMutableCopy();
        }

        checkAndSaveTree(false, checker);
    }
}
