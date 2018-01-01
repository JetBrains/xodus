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
package jetbrains.exodus.gc;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.core.execution.JobProcessor;
import jetbrains.exodus.core.execution.ThreadJobProcessorPool;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.env.TransactionalExecutable;
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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class GarbageCollectorTestInMemory extends GarbageCollectorTest {

    private static final Logger logger = LoggerFactory.getLogger(GarbageCollectorTestInMemory.class);
    private static final int TEST_DURATION = 1000 * 10;

    private final Random rnd = new Random();
    private Memory memory;

    @Override
    protected Pair<DataReader, DataWriter> createRW() throws IOException {
        memory = new Memory();
        return new Pair<DataReader, DataWriter>(new MemoryDataReader(memory), new MemoryDataWriter(memory));
    }

    @Override
    protected void deleteRW() {
        reader = null;
        writer = null;
        memory = null;
    }

    @Test
    public void testTextIndexLike() {
        testTextIndexLike(true);
    }

    @Test
    public void testTextIndexLikeWithoutExpirationChecker() {
        testTextIndexLike(false);
    }

    @Test
    public void testTextIndexLikeWithDeletions() {
        testTextIndexLikeWithDeletions(true);
    }

    @Test
    public void testTextIndexLikeWithDeletionsWithoutExpirationChecker() {
        testTextIndexLikeWithDeletions(false);
    }

    @Test
    public void testTextIndexLikeWithDeletionsAndConcurrentReading() throws InterruptedException, BrokenBarrierException {
        final long started = System.currentTimeMillis();
        prepare();
        final Transaction txn = env.beginTransaction();
        final Store store = env.openStore("store", getStoreConfig(false), txn);
        final Store storeDups = env.openStore("storeDups", getStoreConfig(true), txn);
        txn.commit();
        final Throwable[] throwable = {null};
        final JobProcessor[] processors = new JobProcessor[10];
        for (int i = 0; i < processors.length; ++i) {
            processors[i] = ThreadJobProcessorPool.getOrCreateJobProcessor("test processor" + i);
            processors[i].start();
        }
        final CyclicBarrier barrier = new CyclicBarrier(processors.length + 1);
        processors[0].queue(new Job() {
            @Override
            protected void execute() throws Throwable {
                barrier.await();
                try {
                    while (System.currentTimeMillis() - started < TEST_DURATION) {
                        env.executeInTransaction(new TransactionalExecutable() {
                            @Override
                            public void execute(@NotNull final Transaction txn) {
                                int randomInt = rnd.nextInt() & 0x3fffffff;
                                final int count = 4 + (randomInt) & 0x1f;
                                for (int j = 0; j < count; randomInt += ++j) {
                                    final int intKey = randomInt & 0x3fff;
                                    final ArrayByteIterable key = IntegerBinding.intToCompressedEntry(intKey);
                                    final int valueLength = 50 + (randomInt % 100);
                                    store.put(txn, key, new ArrayByteIterable(new byte[valueLength]));
                                    storeDups.put(txn, key, IntegerBinding.intToEntry(randomInt % 32));
                                }
                                randomInt = (randomInt * randomInt) & 0x3fffffff;
                                for (int j = 0; j < count; randomInt += ++j) {
                                    final int intKey = randomInt & 0x3fff;
                                    final ArrayByteIterable key = IntegerBinding.intToCompressedEntry(intKey);
                                    store.delete(txn, key);
                                    try (Cursor cursor = storeDups.openCursor(txn)) {
                                        if (cursor.getSearchBoth(key, IntegerBinding.intToEntry(randomInt % 32))) {
                                            cursor.deleteCurrent();
                                        }
                                    }
                                }
                            }
                        });
                        Thread.sleep(0);
                    }
                } catch (Throwable t) {
                    throwable[0] = t;
                }
            }
        });
        for (int i = 1; i < processors.length; ++i) {
            processors[i].queue(new Job() {
                @Override
                protected void execute() throws Throwable {
                    try {
                        barrier.await();
                        while (System.currentTimeMillis() - started < TEST_DURATION) {
                            int randomInt = rnd.nextInt() & 0x3fffffff;
                            for (int j = 0; j < 100; randomInt += ++j) {
                                final int intKey = randomInt & 0x3fff;
                                final ArrayByteIterable key = IntegerBinding.intToCompressedEntry(intKey);
                                getAutoCommit(store, key);
                                getAutoCommit(storeDups, key);
                                Thread.sleep(0);
                            }
                            Thread.sleep(50);
                        }
                    } catch (Throwable t) {
                        throwable[0] = t;
                    }
                }
            });
        }
        barrier.await();
        for (final JobProcessor processor : processors) {
            processor.finish();
        }
        final Throwable t = throwable[0];
        if (t != null) {
            memory.dump(new File(System.getProperty("user.home"), "dump"));
            logger.error("User code exception: ", t);
            Assert.assertTrue(false);
        }
    }

    private void testTextIndexLike(boolean useExpirationChecker) {
        final long started = System.currentTimeMillis();
        prepare(useExpirationChecker);
        final Transaction txn = env.beginTransaction();
        final Store store = env.openStore("store", getStoreConfig(false), txn);
        final Store storeDups = env.openStore("storeDups", getStoreConfig(true), txn);
        txn.commit();
        try {
            while (System.currentTimeMillis() - started < TEST_DURATION) {
                env.executeInTransaction(new TransactionalExecutable() {
                    @Override
                    public void execute(@NotNull final Transaction txn) {
                        int randomInt = rnd.nextInt() & 0x3fffffff;
                        final int count = 4 + (randomInt) & 0x1f;
                        for (int j = 0; j < count; randomInt += ++j) {
                            final int intKey = randomInt & 0x3fff;
                            final ArrayByteIterable key = IntegerBinding.intToCompressedEntry(intKey);
                            final int valueLength = 50 + (randomInt % 100);
                            store.put(txn, key, new ArrayByteIterable(new byte[valueLength]));
                            storeDups.put(txn, key, IntegerBinding.intToEntry(randomInt % 32));
                        }
                    }
                });
                Thread.sleep(0);
            }
        } catch (Throwable t) {
            memory.dump(new File(System.getProperty("user.home"), "dump"));
            logger.error("User code exception: ", t);
            Assert.assertTrue(false);
        }
    }

    private void testTextIndexLikeWithDeletions(boolean useExpirationChecker) {
        final long started = System.currentTimeMillis();
        prepare(useExpirationChecker);
        final Transaction txn = env.beginTransaction();
        final Store store = env.openStore("store", getStoreConfig(false), txn);
        final Store storeDups = env.openStore("storeDups", getStoreConfig(true), txn);
        txn.commit();
        try {
            while (System.currentTimeMillis() - started < TEST_DURATION) {
                env.executeInTransaction(new TransactionalExecutable() {
                    @Override
                    public void execute(@NotNull final Transaction txn) {
                        int randomInt = rnd.nextInt() & 0x3fffffff;
                        final int count = 4 + (randomInt) & 0x1f;
                        for (int j = 0; j < count; randomInt += ++j) {
                            final int intKey = randomInt & 0x3fff;
                            final ArrayByteIterable key = IntegerBinding.intToCompressedEntry(intKey);
                            final int valueLength = 50 + (randomInt % 100);
                            store.put(txn, key, new ArrayByteIterable(new byte[valueLength]));
                            storeDups.put(txn, key, IntegerBinding.intToEntry(randomInt % 32));
                        }
                        randomInt = (randomInt * randomInt) & 0x3fffffff;
                        for (int j = 0; j < count / 2; randomInt += ++j) {
                            final int intKey = randomInt & 0x3fff;
                            final ArrayByteIterable key = IntegerBinding.intToCompressedEntry(intKey);
                            store.delete(txn, key);
                            try (Cursor cursor = storeDups.openCursor(txn)) {
                                if (cursor.getSearchBoth(key, IntegerBinding.intToEntry(randomInt % 32))) {
                                    cursor.deleteCurrent();
                                }
                            }
                        }

                    }
                });
                Thread.sleep(0);
            }
        } catch (Throwable t) {
            memory.dump(new File(System.getProperty("user.home"), "dump"));
            logger.error("User code exception: ", t);
            Assert.assertTrue(false);
        }
    }

    private void prepare() {
        prepare(true);
    }

    private void prepare(boolean useExpirationChecker) {
        setLogFileSize(2);
        env.getEnvironmentConfig().setTreeMaxPageSize(16);
        env.getEnvironmentConfig().setGcUseExpirationChecker(useExpirationChecker);
        reopenEnvironment();
    }
}
