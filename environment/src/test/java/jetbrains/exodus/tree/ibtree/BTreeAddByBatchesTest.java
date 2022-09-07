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
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.log.NullLoggable;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

public class BTreeAddByBatchesTest extends BTreeTestBase {
    @Test
    public void testAdd32KByBatchOf1() {
        final long seed = generateSeed();
        System.out.println("testAdd32KByBatchOf1 seed : " + seed);

        t = new ImmutableBTree(log, 1, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 32 * 1024;
        var batchSize = 1;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd32KByBatchOf4() {
        final long seed = generateSeed();
        System.out.println("testAdd32KByBatchOf4 seed : " + seed);

        t = new ImmutableBTree(log, 2, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 32 * 1024;
        var batchSize = 4;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd32KByBatchOf8() {
        final long seed = generateSeed();
        System.out.println("testAdd32KByBatchOf8 seed : " + seed);

        t = new ImmutableBTree(log, 3, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 32 * 1024;
        var batchSize = 8;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd32KByBatchOf64() {
        final long seed = generateSeed();
        System.out.println("testAdd32KByBatchOf64 seed : " + seed);

        t = new ImmutableBTree(log, 4, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 32 * 1024;
        var batchSize = 64;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd32KByBatchOf256() {
        final long seed = generateSeed();
        System.out.println("testAdd32KByBatchOf256 seed : " + seed);

        t = new ImmutableBTree(log, 5, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 32 * 1024;
        var batchSize = 256;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd32KByBatchOf1024() {
        final long seed = generateSeed();
        System.out.println("testAdd32KByBatchOf1024 seed : " + seed);

        t = new ImmutableBTree(log, 6, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 32 * 1024;
        var batchSize = 1024;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd32KByBatchOf4096() {
        final long seed = generateSeed();
        System.out.println("testAdd32KByBatchOf4096 seed : " + seed);

        t = new ImmutableBTree(log, 7, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 32 * 1024;
        var batchSize = 4096;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd64KByBatchOf1() {
        final long seed = generateSeed();
        System.out.println("testAdd64KByBatchOf1 seed : " + seed);

        t = new ImmutableBTree(log, 8, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 64 * 1024;
        var batchSize = 1;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }


    @Test
    public void testAdd64KByBatchOf4() {
        final long seed = generateSeed();
        System.out.println("testAdd64KByBatchOf4 seed : " + seed);

        t = new ImmutableBTree(log, 9, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 64 * 1024;
        var batchSize = 4;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd64KByBatchOf8() {
        final long seed = generateSeed();
        System.out.println("testAdd64KByBatchOf8 seed : " + seed);

        t = new ImmutableBTree(log, 10, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 64 * 1024;
        var batchSize = 8;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd64KByBatchOf64() {
        final long seed = generateSeed();
        System.out.println("testAdd64KByBatchOf64 seed : " + seed);

        t = new ImmutableBTree(log, 11, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 64 * 1024;
        var batchSize = 64;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd64KByBatchOf256() {
        final long seed = generateSeed();
        System.out.println("testAdd64KByBatchOf256 seed : " + seed);

        t = new ImmutableBTree(log, 12, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 64 * 1024;
        var batchSize = 256;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd64KByBatchOf1024() {
        final long seed = generateSeed();
        System.out.println("testAdd64KByBatchOf1024 seed : " + seed);

        t = new ImmutableBTree(log, 13, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 64 * 1024;
        var batchSize = 1024;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAdd64KByBatchOf4096() {
        final long seed = generateSeed();
        System.out.println("testAdd64KByBatchOf4096 seed : " + seed);

        t = new ImmutableBTree(log, 14, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);

        var random = new Random(seed);

        var entriesCount = 64 * 1024;
        var batchSize = 4096;

        addKeysByBatches(entriesCount, batchSize, entriesCount / 10, random);
    }

    @Test
    public void testAddContinuous256KEntriesByBatch1() {
        final long seed = generateSeed();
        System.out.println("testAddContinuous256KEntriesByBatch1 seed : " + seed);
        final Random rnd = new Random(seed);

        int keysCount = 256 * 1024;
        DecimalFormat format = (DecimalFormat) NumberFormat.getIntegerInstance();
        format.applyPattern("00000000");

        final ByteBuffer[] data = new ByteBuffer[keysCount];
        for (int i = 0; i < keysCount; i++) {
            data[i] = StringBinding.stringToEntry(format.format(i)).getByteBuffer();
        }

        t = new ImmutableBTree(log, 15, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        final int batchSize = 1;

        storeDataAndCheck(rnd, data, batchSize);
    }

    @Test
    public void testAddContinuous256KEntriesByBatch4() {
        final long seed = generateSeed();
        System.out.println("testAddContinuous256KEntriesByBatch4 seed : " + seed);
        final Random rnd = new Random(seed);

        int keysCount = 256 * 1024;
        DecimalFormat format = (DecimalFormat) NumberFormat.getIntegerInstance();
        format.applyPattern("00000000");

        final ByteBuffer[] data = new ByteBuffer[keysCount];
        for (int i = 0; i < keysCount; i++) {
            data[i] = StringBinding.stringToEntry(format.format(i)).getByteBuffer();
        }

        t = new ImmutableBTree(log, 16, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        final int batchSize = 4;

        storeDataAndCheck(rnd, data, batchSize);
    }

    @Test
    public void testAddContinuous256KEntriesByBatch8() {
        final long seed = generateSeed();
        System.out.println("testAddContinuous256KEntriesByBatch8 seed : " + seed);
        final Random rnd = new Random(seed);

        int keysCount = 256 * 1024;
        DecimalFormat format = (DecimalFormat) NumberFormat.getIntegerInstance();
        format.applyPattern("00000000");

        final ByteBuffer[] data = new ByteBuffer[keysCount];
        for (int i = 0; i < keysCount; i++) {
            data[i] = StringBinding.stringToEntry(format.format(i)).getByteBuffer();
        }

        t = new ImmutableBTree(log, 17, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        final int batchSize = 8;

        storeDataAndCheck(rnd, data, batchSize);
    }

    @Test
    public void testAddContinuous256KEntriesByBatch64() {
        final long seed = generateSeed();
        System.out.println("testAddContinuous256KEntriesByBatch64 seed : " + seed);
        final Random rnd = new Random(seed);

        int keysCount = 256 * 1024;
        DecimalFormat format = (DecimalFormat) NumberFormat.getIntegerInstance();
        format.applyPattern("00000000");

        final ByteBuffer[] data = new ByteBuffer[keysCount];
        for (int i = 0; i < keysCount; i++) {
            data[i] = StringBinding.stringToEntry(format.format(i)).getByteBuffer();
        }

        t = new ImmutableBTree(log, 18, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        final int batchSize = 64;

        storeDataAndCheck(rnd, data, batchSize);
    }

    @Test
    public void testAddContinuous256KEntriesByBatch256() {
        final long seed = generateSeed();
        System.out.println("testAddContinuous256KEntriesByBatch256 seed : " + seed);
        final Random rnd = new Random(seed);

        int keysCount = 256 * 1024;
        DecimalFormat format = (DecimalFormat) NumberFormat.getIntegerInstance();
        format.applyPattern("00000000");

        final ByteBuffer[] data = new ByteBuffer[keysCount];
        for (int i = 0; i < keysCount; i++) {
            data[i] = StringBinding.stringToEntry(format.format(i)).getByteBuffer();
        }

        t = new ImmutableBTree(log, 19, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        final int batchSize = 256;

        storeDataAndCheck(rnd, data, batchSize);
    }

    @Test
    public void testAddContinuous256KEntriesByBatch4096() {
        final long seed = generateSeed();
        System.out.println("testAddContinuous256KEntriesByBatch4096 seed : " + seed);
        final Random rnd = new Random(seed);

        int keysCount = 256 * 1024;
        DecimalFormat format = (DecimalFormat) NumberFormat.getIntegerInstance();
        format.applyPattern("00000000");

        final ByteBuffer[] data = new ByteBuffer[keysCount];
        for (int i = 0; i < keysCount; i++) {
            data[i] = StringBinding.stringToEntry(format.format(i)).getByteBuffer();
        }

        t = new ImmutableBTree(log, 20, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        final int batchSize = 4096;

        storeDataAndCheck(rnd, data, batchSize);
    }

    @Test
    public void testAddContinuous256KEntriesByBatch1024() {
        final long seed = generateSeed();
        System.out.println("testAddContinuous256KEntriesByBatch1024 seed : " + seed);
        final Random rnd = new Random(seed);

        int keysCount = 256 * 1024;
        DecimalFormat format = (DecimalFormat) NumberFormat.getIntegerInstance();
        format.applyPattern("00000000");

        final ByteBuffer[] data = new ByteBuffer[keysCount];
        for (int i = 0; i < keysCount; i++) {
            data[i] = StringBinding.stringToEntry(format.format(i)).getByteBuffer();
        }

        t = new ImmutableBTree(log, 20, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        final int batchSize = 1024;

        storeDataAndCheck(rnd, data, batchSize);
    }

    @Test
    public void testAddContinuousShuffled256KEntriesByBatch1() {
        final long seed = generateSeed();
        System.out.println("testAddContinuousShuffled256KEntriesByBatch1 seed : " + seed);
        final Random rnd = new Random(seed);

        int keysCount = 256 * 1024;
        DecimalFormat format = (DecimalFormat) NumberFormat.getIntegerInstance();
        format.applyPattern("00000000");

        final List<ByteBuffer> data = new ArrayList<>();
        for (int i = 0; i < keysCount; i++) {
            data.add(StringBinding.stringToEntry(format.format(i)).getByteBuffer());
        }
        Collections.shuffle(data, rnd);

        t = new ImmutableBTree(log, 21, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        final int batchSize = 1;

        storeDataAndCheck(rnd, data.toArray(new ByteBuffer[0]), batchSize);
    }

    @Test
    public void testAddContinuousShuffled256KEntriesByBatch4() {
        final long seed = generateSeed();
        System.out.println("testAddContinuousShuffled256KEntriesByBatch4 seed : " + seed);
        final Random rnd = new Random(seed);

        int keysCount = 256 * 1024;
        DecimalFormat format = (DecimalFormat) NumberFormat.getIntegerInstance();
        format.applyPattern("00000000");

        final List<ByteBuffer> data = new ArrayList<>();
        for (int i = 0; i < keysCount; i++) {
            data.add(StringBinding.stringToEntry(format.format(i)).getByteBuffer());
        }
        Collections.shuffle(data, rnd);

        t = new ImmutableBTree(log, 22, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        final int batchSize = 4;

        storeDataAndCheck(rnd, data.toArray(new ByteBuffer[0]), batchSize);
    }

    @Test
    public void testAddContinuousShuffled256KEntriesByBatch8() {
        final long seed = generateSeed();
        System.out.println("testAddContinuousShuffled256KEntriesByBatch8 seed : " + seed);
        final Random rnd = new Random(seed);

        int keysCount = 256 * 1024;
        DecimalFormat format = (DecimalFormat) NumberFormat.getIntegerInstance();
        format.applyPattern("00000000");

        final List<ByteBuffer> data = new ArrayList<>();
        for (int i = 0; i < keysCount; i++) {
            data.add(StringBinding.stringToEntry(format.format(i)).getByteBuffer());
        }
        Collections.shuffle(data, rnd);

        t = new ImmutableBTree(log, 23, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        final int batchSize = 8;

        storeDataAndCheck(rnd, data.toArray(new ByteBuffer[0]), batchSize);
    }

    @Test
    public void testAddContinuousShuffled256KEntriesByBatch64() {
        final long seed = generateSeed();
        System.out.println("testAddContinuousShuffled256KEntriesByBatch64 seed : " + seed);
        final Random rnd = new Random(seed);

        int keysCount = 256 * 1024;
        DecimalFormat format = (DecimalFormat) NumberFormat.getIntegerInstance();
        format.applyPattern("00000000");

        final List<ByteBuffer> data = new ArrayList<>();
        for (int i = 0; i < keysCount; i++) {
            data.add(StringBinding.stringToEntry(format.format(i)).getByteBuffer());
        }
        Collections.shuffle(data, rnd);

        t = new ImmutableBTree(log, 23, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        final int batchSize = 64;

        storeDataAndCheck(rnd, data.toArray(new ByteBuffer[0]), batchSize);
    }

    @Test
    public void testAddContinuousShuffled256KEntriesByBatch256() {
        final long seed = generateSeed();
        System.out.println("testAddContinuousShuffled256KEntriesByBatch256 seed : " + seed);
        final Random rnd = new Random(seed);

        int keysCount = 256 * 1024;
        DecimalFormat format = (DecimalFormat) NumberFormat.getIntegerInstance();
        format.applyPattern("00000000");

        final List<ByteBuffer> data = new ArrayList<>();
        for (int i = 0; i < keysCount; i++) {
            data.add(StringBinding.stringToEntry(format.format(i)).getByteBuffer());
        }
        Collections.shuffle(data, rnd);

        t = new ImmutableBTree(log, 23, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        final int batchSize = 256;

        storeDataAndCheck(rnd, data.toArray(new ByteBuffer[0]), batchSize);
    }

    @Test
    public void testAddContinuousShuffled256KEntriesByBatch1024() {
        final long seed = generateSeed();
        System.out.println("testAddContinuousShuffled256KEntriesByBatch1024 seed : " + seed);
        final Random rnd = new Random(seed);

        int keysCount = 256 * 1024;
        DecimalFormat format = (DecimalFormat) NumberFormat.getIntegerInstance();
        format.applyPattern("00000000");

        final List<ByteBuffer> data = new ArrayList<>();
        for (int i = 0; i < keysCount; i++) {
            data.add(StringBinding.stringToEntry(format.format(i)).getByteBuffer());
        }
        Collections.shuffle(data, rnd);

        t = new ImmutableBTree(log, 24, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        final int batchSize = 1024;

        storeDataAndCheck(rnd, data.toArray(new ByteBuffer[0]), batchSize);
    }

    @Test
    public void testAddContinuousShuffled256KEntriesByBatch4096() {
        final long seed = generateSeed();
        System.out.println("testAddContinuousShuffled256KEntriesByBatch4096 seed : " + seed);
        final Random rnd = new Random(seed);

        int keysCount = 256 * 1024;
        DecimalFormat format = (DecimalFormat) NumberFormat.getIntegerInstance();
        format.applyPattern("00000000");

        final List<ByteBuffer> data = new ArrayList<>();
        for (int i = 0; i < keysCount; i++) {
            data.add(StringBinding.stringToEntry(format.format(i)).getByteBuffer());
        }
        Collections.shuffle(data, rnd);

        t = new ImmutableBTree(log, 24, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        final int batchSize = 4096;

        storeDataAndCheck(rnd, data.toArray(new ByteBuffer[0]), batchSize);
    }

    private void storeDataAndCheck(Random rnd, ByteBuffer[] data, int batchSize) {
        tm = t.getMutableCopy();
        final TreeMap<ByteBuffer, ByteBuffer> expectedMap = new TreeMap<>(ByteBufferComparator.INSTANCE);

        int stored = 0;
        int prevStored = 0;

        for (ByteBuffer buffer : data) {
            tm.put(buffer, buffer);
            expectedMap.put(buffer, buffer);

            stored++;
            if (stored - prevStored == batchSize) {
                var structureId = t.getStructureId();
                long address = saveTree();
                t = openTree(address, false, structureId);
                tm = t.getMutableCopy();
                prevStored = stored;
            }
        }

        checkAndSaveTree(false, new ImmutableTreeChecker(expectedMap, rnd));
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
