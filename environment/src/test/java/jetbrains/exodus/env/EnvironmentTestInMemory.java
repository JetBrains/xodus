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
package jetbrains.exodus.env;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.persistent.Persistent23TreeMap;
import jetbrains.exodus.io.DataReader;
import jetbrains.exodus.io.DataWriter;
import jetbrains.exodus.io.inMemory.Memory;
import jetbrains.exodus.io.inMemory.MemoryDataReader;
import jetbrains.exodus.io.inMemory.MemoryDataWriter;
import jetbrains.exodus.util.Random;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class EnvironmentTestInMemory extends EnvironmentTest {

    private static final Logger logger = LoggerFactory.getLogger(EnvironmentTestInMemory.class);
    private static final int TEST_DURATION = 1000 * 15;

    private final Random rnd = new Random();

    @Override
    protected Pair<DataReader, DataWriter> createRW() throws IOException {
        Memory memory = new Memory();
        return new Pair<DataReader, DataWriter>(new MemoryDataReader(memory), new MemoryDataWriter(memory));
    }

    @Override
    protected void deleteRW() {
        reader = null;
        writer = null;
    }

    @Test
    public void failedToPut() throws InterruptedException {
        prepare();
        final int keysCount = 50000;
        final int valuesCount = 20;
        final long started = System.currentTimeMillis();
        final Persistent23TreeMap<Integer, Integer> testMap = new Persistent23TreeMap<>();
        final Store primary = openStoreAutoCommit("primary", StoreConfig.WITHOUT_DUPLICATES);
        final Store secondary = openStoreAutoCommit("secondary", StoreConfig.WITH_DUPLICATES);
        while (System.currentTimeMillis() - started < TEST_DURATION) {
            if (rnd.nextInt() % 100 == 1) {
                Thread.sleep(101);
            }
            final Transaction txn = env.beginTransaction();
            final Persistent23TreeMap.MutableMap<Integer, Integer> mutableMap = testMap.beginWrite();
            try {
                for (int i = 0; i < 10; ++i) {
                    putRandomKeyValue(primary, secondary, txn, keysCount, valuesCount, mutableMap);
                    deleteRandomKey(primary, secondary, txn, keysCount, mutableMap);
                }
                if (txn.flush()) {
                    mutableMap.endWrite();
                }
            } catch (Throwable t) {
                logger.error("Failed to put", t);
                break;
            } finally {
                txn.abort();
            }
        }
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                try (Cursor cursor = primary.openCursor(txn)) {
                    Assert.assertTrue(cursor.getNext());
                    for (Persistent23TreeMap.Entry<Integer, Integer> entry : testMap.beginRead()) {
                        Assert.assertEquals((int) entry.getKey(), IntegerBinding.readCompressed(cursor.getKey().iterator()));
                        Assert.assertEquals((int) entry.getValue(), IntegerBinding.readCompressed(cursor.getValue().iterator()));
                        cursor.getNext();
                    }
                }
            }
        });
    }

    private void putRandomKeyValue(Store primary,
                                   Store secondary,
                                   Transaction txn,
                                   int keysCount,
                                   int valuesCount,
                                   Persistent23TreeMap.MutableMap<Integer, Integer> testMap) {
        final int key = rnd.nextInt(keysCount);
        final ArrayByteIterable keyEntry = IntegerBinding.intToCompressedEntry(key);
        final int value = rnd.nextInt(valuesCount);
        testMap.put(key, value);
        final ArrayByteIterable valueEntry = IntegerBinding.intToCompressedEntry(value);
        final ByteIterable oldValue = primary.get(txn, keyEntry);
        primary.put(txn, keyEntry, valueEntry);
        if (oldValue != null) {
            try (Cursor cursor = secondary.openCursor(txn)) {
                Assert.assertTrue(cursor.getSearchBoth(oldValue, keyEntry));
                Assert.assertTrue(cursor.deleteCurrent());
            }
        }
        secondary.put(txn, valueEntry, keyEntry);
    }

    private void deleteRandomKey(Store primary,
                                 Store secondary,
                                 Transaction txn,
                                 int keysCount,
                                 Persistent23TreeMap.MutableMap<Integer, Integer> testMap) {
        final int key = rnd.nextInt(keysCount);
        testMap.remove(key);
        final ArrayByteIterable keyEntry = IntegerBinding.intToCompressedEntry(key);
        final ByteIterable oldValue = primary.get(txn, keyEntry);
        primary.delete(txn, keyEntry);
        if (oldValue != null) {
            try (Cursor cursor = secondary.openCursor(txn)) {
                Assert.assertTrue(cursor.getSearchBoth(oldValue, keyEntry));
                Assert.assertTrue(cursor.deleteCurrent());
            }
        }
    }

    private void prepare() {
        setLogFileSize(4096);
        env.getEnvironmentConfig().setTreeMaxPageSize(16);
        reopenEnvironment();
    }
}
