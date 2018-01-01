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
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.*;
import jetbrains.exodus.log.Log;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

public class GarbageCollectorInterleavingTest extends EnvironmentTestsBase {

    @Test
    public void testSimple() throws InterruptedException {
        set1KbFileWithoutGC();

        final Log log = env.getLog();
        final long fileSize = log.getFileSize() * 1024;

        fill("updateSameKey");
        Assert.assertEquals(1L, log.getNumberOfFiles());
        fill("updateSameKey");

        Assert.assertEquals(2L, log.getNumberOfFiles()); // but ends in second one

        fill("another");

        Assert.assertEquals(3L, log.getNumberOfFiles()); // make cleaning of second file possible

        env.getGC().doCleanFile(fileSize); // clean second file

        Thread.sleep(300);
        env.getGC().testDeletePendingFiles();

        Assert.assertEquals(3L, log.getNumberOfFiles()); // half of tree written out from second file

        env.getGC().doCleanFile(0); // clean first file

        Thread.sleep(300);
        env.getGC().testDeletePendingFiles();

        Assert.assertEquals(2L, log.getNumberOfFiles()); // first file contained only garbage

        check("updateSameKey");
        check("another");
    }

    private void fill(@NotNull final String table) {
        final ByteIterable val0 = StringBinding.stringToEntry("val0");
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                final StoreImpl store = env.openStore(table, getStoreConfig(), txn);
                for (int i = 0; i < getRecordsNumber(); ++i) {
                    final ArrayByteIterable key = StringBinding.stringToEntry("key " + i);
                    store.put(txn, key, val0);
                }
            }
        });
    }

    private void check(@NotNull final String table) {
        final ByteIterable val0 = StringBinding.stringToEntry("val0");
        env.executeInReadonlyTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                final Store store = env.openStore(table, getStoreConfig(), txn);
                for (int i = 0; i < getRecordsNumber(); ++i) {
                    final ArrayByteIterable key = StringBinding.stringToEntry("key " + i);
                    Assert.assertTrue(store.exists(txn, key, val0));
                }
            }
        });
    }

    protected StoreConfig getStoreConfig() {
        return StoreConfig.WITHOUT_DUPLICATES;
    }

    protected int getRecordsNumber() {
        return 37;
    }
}
