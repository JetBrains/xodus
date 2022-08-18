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
import jetbrains.exodus.log.LoggableIterator;
import jetbrains.exodus.log.NullLoggable;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

public class BTreeReclaimTest extends BTreeTestBase {
    @Test
    public void testChangeSingleEntry() {
        final long seed = System.nanoTime();
        System.out.println("testChangeSingleEntry : " + seed);

        t = new ImmutableBTree(log, 1, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        add64KAndChangeEntriesThenReclaim(1, rnd);
    }

    @Test
    public void testChange4Entries() {
        final long seed = System.nanoTime();
        System.out.println("testChange4Entries : " + seed);

        t = new ImmutableBTree(log, 2, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        add64KAndChangeEntriesThenReclaim(4, rnd);
    }

    @Test
    public void testChange64Entries() {
        final long seed = System.nanoTime();
        System.out.println("testChange64Entries : " + seed);

        t = new ImmutableBTree(log, 3, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        add64KAndChangeEntriesThenReclaim(64, rnd);
    }

    @Test
    public void testChange256Entries() {
        final long seed = System.nanoTime();
        System.out.println("testChange256Entries : " + seed);

        t = new ImmutableBTree(log, 4, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        add64KAndChangeEntriesThenReclaim(256, rnd);
    }

    @Test
    public void testChange1KEntries() {
        final long seed = System.nanoTime();
        System.out.println("testChange1KEntries : " + seed);

        t = new ImmutableBTree(log, 5, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        add64KAndChangeEntriesThenReclaim(1024, rnd);
    }

    @Test
    public void testChange16KEntries() {
        final long seed = System.nanoTime();
        System.out.println("testChange16KEntries : " + seed);

        t = new ImmutableBTree(log, 6, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        add64KAndChangeEntriesThenReclaim(16 * 1024, rnd);
    }

    @Test
    public void testChange64Entries4Times() {
        final long seed = System.nanoTime();
        System.out.println("testChange64Entries4Times : " + seed);

        t = new ImmutableBTree(log, 7, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        add64EntriesChange64NTimes(4, rnd);
    }

    @Test
    public void testChange64Entries64Times() {
        final long seed = System.nanoTime();
        System.out.println("testChange64Entries64Times : " + seed);

        t = new ImmutableBTree(log, 7, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        add64EntriesChange64NTimes(64, rnd);
    }

    @Test
    public void testChange64Entries256Times() {
        final long seed = System.nanoTime();
        System.out.println("testChange64Entries256Times : " + seed);

        t = new ImmutableBTree(log, 8, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        add64EntriesChange64NTimes(256, rnd);
    }

    @Test
    public void testChange64Entries1KTimes() {
        final long seed = System.nanoTime();
        System.out.println("testChange64Entries1KTimes : " + seed);

        t = new ImmutableBTree(log, 9, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        add64EntriesChange64NTimes(1024, rnd);
    }


    @Test
    public void testAddContinuesKeyByBatchChange256AndReclaim() {
        final long seed = System.nanoTime();
        System.out.println("testAddContinuesKeyByBatchChange256AndReclaim : " + seed);

        t = new ImmutableBTree(log, 9, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        addContinuousKeysByBatchAndReclaimTree(256, 1, rnd);
    }

    @Test
    public void testAddContinuesKeyByBatchChange256By4TimesAndReclaim() {
        final long seed = System.nanoTime();
        System.out.println("testAddContinuesKeyByBatchChange256By4TimesAndReclaim : " + seed);

        t = new ImmutableBTree(log, 9, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        addContinuousKeysByBatchAndReclaimTree(256, 4, rnd);
    }

    @Test
    public void testAddContinuesKeyByBatchChange256By64TimesAndReclaim() {
        final long seed = System.nanoTime();
        System.out.println("testAddContinuesKeyByBatchChange256By64TimesAndReclaim : " + seed);

        t = new ImmutableBTree(log, 9, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        addContinuousKeysByBatchAndReclaimTree(256, 64, rnd);
    }

    @Test
    public void testAddContinuesKeyByBatchChange256By256TimesAndReclaim() {
        final long seed = System.nanoTime();
        System.out.println("testAddContinuesKeyByBatchChange256By256TimesAndReclaim : " + seed);

        t = new ImmutableBTree(log, 9, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        addContinuousKeysByBatchAndReclaimTree(256, 256, rnd);
    }

    @Test
    public void testAddContinuesKeyByBatchChange256By1024TimesAndReclaim() {
        final long seed = System.nanoTime();
        System.out.println("testAddContinuesKeyByBatchChange256By1024TimesAndReclaim : " + seed);

        t = new ImmutableBTree(log, 9, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        addContinuousKeysByBatchAndReclaimTree(256, 1024, rnd);
    }

    @Test
    public void testAddContinuesKeyByBatchChange1024AndReclaim() {
        final long seed = System.nanoTime();
        System.out.println("testAddContinuesKeyByBatchChange1024AndReclaim : " + seed);

        t = new ImmutableBTree(log, 9, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        addContinuousKeysByBatchAndReclaimTree(1024, 1, rnd);
    }

    @Test
    public void testAddContinuesKeyByBatchChange1024By4TimesAndReclaim() {
        final long seed = System.nanoTime();
        System.out.println("testAddContinuesKeyByBatchChange1024By4TimesAndReclaim : " + seed);

        t = new ImmutableBTree(log, 9, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        addContinuousKeysByBatchAndReclaimTree(1024, 4, rnd);
    }

    @Test
    public void testAddContinuesKeyByBatchChange1024By64TimesAndReclaim() {
        final long seed = System.nanoTime();
        System.out.println("testAddContinuesKeyByBatchChange1024By64TimesAndReclaim : " + seed);

        t = new ImmutableBTree(log, 9, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        addContinuousKeysByBatchAndReclaimTree(1024, 64, rnd);
    }

    @Test
    public void testAddContinuesKeyByBatchChange1024By256TimesAndReclaim() {
        final long seed = System.nanoTime();
        System.out.println("testAddContinuesKeyByBatchChange1024By256TimesAndReclaim : " + seed);

        t = new ImmutableBTree(log, 9, log.getCachePageSize(), NullLoggable.NULL_ADDRESS);
        var rnd = new Random(seed);

        addContinuousKeysByBatchAndReclaimTree(1024, 256, rnd);
    }


    private void addContinuousKeysByBatchAndReclaimTree(final int entriesToChange, final int iterations,
                                                        final Random rnd) {
        int keysCount = 1_000_000;
        DecimalFormat format = (DecimalFormat) NumberFormat.getIntegerInstance();
        format.applyPattern("00000000");

        final List<ByteBuffer> data = new ArrayList<>();
        for (int i = 0; i < keysCount; i++) {
            data.add(StringBinding.stringToEntry(format.format(i)).getByteBuffer());
        }

        tm = t.getMutableCopy();
        final TreeMap<ByteBuffer, ByteBuffer> expectedMap = new TreeMap<>(ByteBufferComparator.INSTANCE);
        for (var key : data) {
            expectedMap.put(key, key);
            tm.put(key, key);
        }


        var structureId = t.getStructureId();
        long address = saveTree();
        reopen();

        openTree(address, false, structureId);
        checkBTreeAddresses(0);

        for (int i = 0; i < iterations; i++) {
            address = changeEntries(entriesToChange, rnd, expectedMap);
            openTree(address, false, structureId);
        }


        reclaimTree(rnd, expectedMap, structureId, address);
    }

    private void add64EntriesChange64NTimes(final int iterations, final Random random) {
        final TreeMap<ByteBuffer, ByteBuffer> expectedMap = new TreeMap<>(ByteBufferComparator.INSTANCE);
        int structureId = addEntries(random, expectedMap);
        long address = -1;

        for (int i = 0; i < iterations; i++) {
            address = changeEntries(64, random, expectedMap);
            openTree(address, false, structureId);
        }

        reclaimTree(random, expectedMap, structureId, address);
    }

    private void add64KAndChangeEntriesThenReclaim(final int entriesToChange, final Random random) {
        final TreeMap<ByteBuffer, ByteBuffer> expectedMap = new TreeMap<>(ByteBufferComparator.INSTANCE);
        int structureId = addEntries(random, expectedMap);
        long address = changeEntries(entriesToChange, random, expectedMap);

        reclaimTree(random, expectedMap, structureId, address);
    }

    private void reclaimTree(Random random, TreeMap<ByteBuffer, ByteBuffer> expectedMap, int structureId, long address) {

        var highAddress = log.getHighAddress();
        var fileLengthBound = log.getFileLengthBound();
        var fileSize = log.getFileSize(highAddress / fileLengthBound);
        var fileReminder = fileLengthBound - fileSize;

        log.beginWrite();
        for (long i = 0; i < fileReminder; i++) {
            log.write(NullLoggable.create());
        }
        log.flush();
        log.endWrite();

        highAddress = log.getHighAddress();
        var filesCount = (highAddress + fileLengthBound - 1) / fileLengthBound;
        Assert.assertTrue(filesCount > 0);

        openTree(address, false, structureId);
        tm = t.getMutableCopy();

        System.out.println("Reclaim " + filesCount + " files");

        for (long i = 0; i < filesCount; i++) {
            var fileAddress = i * fileLengthBound;
            var nextFileAddress = fileAddress + fileLengthBound;

            var loggables = log.getLoggableIterator(fileAddress);
            while (loggables.hasNext()) {
                var loggable = loggables.next();
                if (loggable == null || loggable.getAddress() >= nextFileAddress) {
                    break;
                }
                if (loggable.getStructureId() > 0) {
                    tm.reclaim(loggable, loggables, fileLengthBound);
                }
            }
        }

        address = saveTree();
        openTree(address, false, structureId);

        checkBTreeAddresses(highAddress);

        var checker = new ImmutableTreeChecker(expectedMap, random);
        checker.accept(t);
    }

    private long changeEntries(int entriesToChange, Random random, TreeMap<ByteBuffer, ByteBuffer> expectedMap) {
        long address;
        tm = t.getMutableCopy();
        var keys = new ArrayList<>(expectedMap.keySet());
        Collections.shuffle(keys);

        var keysIterator = keys.iterator();

//        for (int i = 0; i < entriesToChange; i++) {
//            var key = keysIterator.next();
//            tm.delete(key);
//            expectedMap.remove(key);
//        }

        for (int i = 0; i < entriesToChange; i++) {
            var key = keysIterator.next();
            var value = tm.get(key);

            Assert.assertNotNull(value);

            var bt = value.get(0);

            var newValue = ByteBuffer.allocate(value.limit());
            newValue.put(0, value, 0, value.limit());

            newValue.put(0, (byte) (bt + 1));

            tm.put(key, newValue);
            expectedMap.put(key, newValue);
        }

        for (int i = 0; i < entriesToChange; i++) {
            while (true) {
                var keySize = random.nextInt(1, 16);
                var keyArray = new byte[keySize];
                random.nextBytes(keyArray);

                var valueArray = new byte[32];
                random.nextBytes(valueArray);

                var key = ByteBuffer.wrap(keyArray);
                var value = ByteBuffer.wrap(valueArray);

                if (!expectedMap.containsKey(key)) {
                    expectedMap.put(key, value);
                    tm.put(key, value);
                    break;
                }
            }
        }

        address = saveTree();
        return address;
    }

    private int addEntries(Random random, TreeMap<ByteBuffer, ByteBuffer> expectedMap) {
        final int initialEntries = 64 * 1024;
        tm = t.getMutableCopy();
        for (int i = 0; i < initialEntries; i++) {
            var keySize = random.nextInt(1, 16);
            var keyArray = new byte[keySize];
            random.nextBytes(keyArray);

            var valueArray = new byte[32];
            random.nextBytes(valueArray);

            var key = ByteBuffer.wrap(keyArray);
            var value = ByteBuffer.wrap(valueArray);

            expectedMap.put(key, value);
            tm.put(key, value);
        }

        var structureId = t.getStructureId();
        long address = saveTree();
        reopen();

        openTree(address, false, structureId);
        checkBTreeAddresses(0);
        return structureId;
    }

    private void checkBTreeAddresses(long startAddress) {
        Set<Long> loggableAddresses = new HashSet<>();

        var loggableIterator = new LoggableIterator(log, startAddress);
        while (loggableIterator.hasNext()) {
            var loggable = loggableIterator.next();
            assert loggable != null;
            if (loggable.getStructureId() > 0) {
                loggableAddresses.add(loggable.getAddress());
            }
        }

        var addressIterator = t.addressIterator();
        int addressCount = 0;

        while (addressIterator.hasNext()) {
            addressCount++;
            var currentAddress = addressIterator.next();
            Assert.assertTrue(loggableAddresses.contains(currentAddress));
        }

        Assert.assertEquals(loggableAddresses.size(), addressCount);
    }
}
