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
import jetbrains.exodus.log.LoggableIterator;
import jetbrains.exodus.log.NullLoggable;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

public class BTreeReclaimTest extends BasicBTreeTest {
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

    private void add64KAndChangeEntriesThenReclaim(final int entriesToChange, final Random random) {
        final int initialEntries = 64 * 1024;

        final TreeMap<ByteBuffer, ByteBuffer> expectedMap = new TreeMap<>(ByteBufferComparator.INSTANCE);

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

        tm = t.getMutableCopy();

        var keys = new ArrayList<>(expectedMap.keySet());
        Collections.shuffle(keys);

        var keysIterator = keys.iterator();

        for (int i = 0; i < entriesToChange; i++) {
            var key = keysIterator.next();
            tm.delete(key);
            expectedMap.remove(key);
        }

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
        var filesCount = highAddress / fileLengthBound;
        Assert.assertTrue(filesCount > 0);

        openTree(address, false, structureId);
        tm = t.getMutableCopy();

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
