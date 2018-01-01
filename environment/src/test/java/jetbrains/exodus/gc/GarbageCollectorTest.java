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
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.*;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogUtil;
import jetbrains.exodus.log.RandomAccessLoggable;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class GarbageCollectorTest extends EnvironmentTestsBase {

    @Test
    public void dummyCleanWholeLog() {
        getEnvDirectory();
        GarbageCollector gc = env.getGC();
        gc.cleanWholeLog();
        gc.doCleanFile(env.getLog().getHighFileAddress());
    }

    @Test
    public void reclaimTreeWithRootInLastFile() {
        set1KbFileWithoutGC();
        final Log log = env.getLog();
        final long startAddress = createStore("store", 100);
        Assert.assertEquals(2, log.getNumberOfFiles());
        createStore("corrupted", 160);
        Assert.assertEquals(4, log.getNumberOfFiles());
        log.removeFile(2 * log.getFileSize() * LogUtil.LOG_BLOCK_ALIGNMENT);
        final StoreImpl store = openStoreAutoCommit("store");
        final Iterator<RandomAccessLoggable> itr = log.getLoggableIterator(startAddress);
        final TransactionBase txn = env.beginTransaction();
        Assert.assertTrue(txn.getTree(store).getMutableCopy().reclaim(itr.next(), itr));
        txn.abort();
    }

    @Test
    public void updateSameKeyWithoutDuplicates() {
        set1KbFileWithoutGC();
        ByteIterable key = StringBinding.stringToEntry("key");
        final Store store = openStoreAutoCommit("updateSameKey");
        for (int i = 0; i < 1000; ++i) {
            putAutoCommit(store, key, key);
        }
        Assert.assertTrue(env.getLog().getNumberOfFiles() > 1);

        env.getGC().cleanWholeLog();

        Assert.assertEquals(1L, env.getLog().getNumberOfFiles());
    }

    @Test
    public void updateSameKeyWithDuplicates() {
        set1KbFileWithoutGC();
        ByteIterable key = StringBinding.stringToEntry("key");
        final Store store = openStoreAutoCommit("updateSameKey", getStoreConfig(true));
        for (int i = 0; i < 1000; ++i) {
            putAutoCommit(store, key, key);
        }
        Assert.assertTrue(env.getLog().getNumberOfFiles() > 1);

        env.getGC().cleanWholeLog();

        Assert.assertEquals(1L, env.getLog().getNumberOfFiles());
    }

    @Test
    public void updateSameKeyDeleteWithoutDuplicates() {
        set1KbFileWithoutGC();
        ByteIterable key = StringBinding.stringToEntry("key");
        final Store store = openStoreAutoCommit("updateSameKey");
        for (int i = 0; i < 1000; ++i) {
            putAutoCommit(store, key, key);
        }
        deleteAutoCommit(store, key);
        Assert.assertTrue(env.getLog().getNumberOfFiles() > 1);

        env.getGC().cleanWholeLog();

        Assert.assertEquals(1L, env.getLog().getNumberOfFiles());
    }

    @Test
    public void updateSameKeyDeleteWithDuplicates() {
        set1KbFileWithoutGC();
        ByteIterable key = StringBinding.stringToEntry("key");
        final Store store = openStoreAutoCommit("updateSameKey", getStoreConfig(true));
        for (int i = 0; i < 1000; ++i) {
            putAutoCommit(store, key, key);
        }
        deleteAutoCommit(store, key);
        Assert.assertTrue(env.getLog().getNumberOfFiles() > 1);

        env.getGC().cleanWholeLog();

        Assert.assertEquals(1L, env.getLog().getNumberOfFiles());
    }

    @Test
    public void reopenDbAfterGc() {
        set1KbFileWithoutGC();
        ByteIterable key = StringBinding.stringToEntry("key");
        Store store = openStoreAutoCommit("updateSameKey");
        for (int i = 0; i < 1000; ++i) {
            putAutoCommit(store, key, key);
        }
        Assert.assertEquals(1, countAutoCommit(store));

        env.getGC().cleanWholeLog();

        store = openStoreAutoCommit("updateSameKey", StoreConfig.USE_EXISTING);
        Assert.assertEquals(1, countAutoCommit(store));

        reopenEnvironment();

        store = openStoreAutoCommit("updateSameKey", StoreConfig.USE_EXISTING);
        Assert.assertEquals(1, countAutoCommit(store));
    }

    @Test
    public void reopenDbAfterGcWithBackgroundCleaner() {
        set1KbFileWithoutGC();
        env.getEnvironmentConfig().setGcEnabled(true); // enable background GC
        ByteIterable key = StringBinding.stringToEntry("key");
        Store store = openStoreAutoCommit("updateSameKey");
        for (int i = 0; i < 1000; ++i) {
            putAutoCommit(store, key, key);
        }
        Assert.assertEquals(1, countAutoCommit(store));

        env.getGC().cleanWholeLog();

        store = openStoreAutoCommit("updateSameKey", StoreConfig.USE_EXISTING);
        Assert.assertEquals(1, countAutoCommit(store));

        reopenEnvironment();

        store = openStoreAutoCommit("updateSameKey", StoreConfig.USE_EXISTING);
        Assert.assertEquals(1, countAutoCommit(store));
    }

    @Test
    public void reopenDbAfterGcWithBackgroundCleanerCyclic() throws Exception {
        for (int i = 0; i < 8; i++) {
            reopenDbAfterGcWithBackgroundCleaner();
            tearDown();
            setUp();
        }
    }

    @Test
    public void fillDuplicatesThenDeleteAlmostAllOfThem() {
        set1KbFileWithoutGC();

        Store store = openStoreAutoCommit("duplicates", getStoreConfig(true));
        for (int i = 0; i < 32; ++i) {
            for (int j = 0; j < 32; ++j) {
                putAutoCommit(store, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(j));
            }
        }
        for (int i = 0; i < 32; ++i) {
            final Transaction txn = env.beginTransaction();
            try {
                try (Cursor cursor = store.openCursor(txn)) {
                    Assert.assertNotNull(cursor.getSearchKeyRange(IntegerBinding.intToEntry(i)));
                    for (int j = 0; j < 32; ++j, cursor.getNext()) {
                        cursor.deleteCurrent();
                    }
                }
                store.put(txn, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(100));
            } finally {
                txn.commit();
            }
        }
        env.getGC().cleanWholeLog();

        reopenEnvironment();

        store = openStoreAutoCommit("duplicates", getStoreConfig(true));
        for (int i = 0; i < 32; ++i) {
            final ByteIterable it = getAutoCommit(store, IntegerBinding.intToEntry(i));
            Assert.assertNotNull(it);
            Assert.assertEquals(100, IntegerBinding.entryToInt(it));
        }
    }

    @Test
    public void fillDuplicatesWithoutDuplicates() {
        set1KbFileWithoutGC();
        Store dups = openStoreAutoCommit("duplicates", getStoreConfig(true));
        putAutoCommit(dups, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(0));
        putAutoCommit(dups, IntegerBinding.intToEntry(1), IntegerBinding.intToEntry(0));
        putAutoCommit(dups, IntegerBinding.intToEntry(1), IntegerBinding.intToEntry(1));
        Store nodups = openStoreAutoCommit("no duplicates");
        for (int i = 0; i < 1000; ++i) {
            putAutoCommit(nodups, IntegerBinding.intToEntry(0), IntegerBinding.intToEntry(i));
        }

        env.getGC().cleanWholeLog();

        reopenEnvironment();

        dups = openStoreAutoCommit("duplicates", getStoreConfig(true));
        Assert.assertNotNull(getAutoCommit(dups, IntegerBinding.intToEntry(0)));
        Assert.assertNotNull(getAutoCommit(dups, IntegerBinding.intToEntry(1)));
        Assert.assertNull(getAutoCommit(dups, IntegerBinding.intToEntry(2)));
    }

    @Test
    public void truncateStore() {
        set2KbFileWithoutGC(); // patricia root loggable with 250+ children can't fit one kb
        Store store = openStoreAutoCommit("store");
        for (int i = 0; i < 1000; ++i) {
            putAutoCommit(store, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(i));
        }
        Assert.assertTrue(env.getLog().getNumberOfFiles() > 1);

        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                env.truncateStore("store", txn);
            }
        });

        env.getGC().cleanWholeLog();

        Assert.assertEquals(1L, env.getLog().getNumberOfFiles());
    }

    @Test
    public void removeStore() {
        set2KbFileWithoutGC(); // patricia root loggable with 250+ children can't fit one kb
        Store store = openStoreAutoCommit("store");
        for (int i = 0; i < 1000; ++i) {
            putAutoCommit(store, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(i));
        }
        Assert.assertTrue(env.getLog().getNumberOfFiles() > 1);

        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                env.removeStore("store", txn);
            }
        });

        env.getGC().cleanWholeLog();

        Assert.assertEquals(1L, env.getLog().getNumberOfFiles());
    }

    @Test
    public void removeStoreCreateStoreGet() {
        set2KbFileWithoutGC(); // patricia root loggable with 250+ children can't fit one kb
        Store store = openStoreAutoCommit("store");
        final int count = 500;
        for (int i = 0; i < count; ++i) {
            putAutoCommit(store, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(i));
        }
        Assert.assertTrue(env.getLog().getNumberOfFiles() > 1);

        env.getGC().cleanWholeLog();

        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                env.removeStore("store", txn);
            }
        });

        store = openStoreAutoCommit("store");
        for (int i = 0; i < count; ++i) {
            putAutoCommit(store, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(i));
        }

        env.getGC().cleanWholeLog();

        for (int i = 0; i < count; ++i) {
            Assert.assertEquals(i, IntegerBinding.entryToInt(getAutoCommit(store, IntegerBinding.intToEntry(i))));
        }
    }


    @Test
    public void xd98() {
        setLogFileSize(64);
        env.getEnvironmentConfig().setGcEnabled(false);
        final Store store = openStoreAutoCommit("store");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 100; ++i) {
            builder.append("01234567890123456789012345678901234567890123456789");
            final ArrayByteIterable value = StringBinding.stringToEntry(builder.toString());
            putAutoCommit(store, IntegerBinding.intToEntry(0), value);
        }
        env.getGC().cleanWholeLog();
        Assert.assertEquals(1L, env.getLog().getNumberOfFiles());
    }

    protected StoreImpl openStoreAutoCommit(final String name) {
        return (StoreImpl) openStoreAutoCommit(name, getStoreConfig(false));
    }

    protected StoreConfig getStoreConfig(boolean hasDuplicates) {
        return hasDuplicates ? StoreConfig.WITH_DUPLICATES : StoreConfig.WITHOUT_DUPLICATES;
    }

    private long createStore(@NotNull final String name, final int keys) {
        final Transaction txn = env.beginTransaction();
        final Store store = env.openStore(name, getStoreConfig(false), txn);
        for (int i = 0; i < keys; ++i) {
            store.put(txn, IntegerBinding.intToEntry(i), IntegerBinding.intToEntry(i));
        }
        final long result = env.getLog().getHighAddress();
        txn.commit();
        return result;
    }
}
