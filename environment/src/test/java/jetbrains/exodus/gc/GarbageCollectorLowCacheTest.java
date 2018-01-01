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

import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.EnvironmentTestsBase;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogConfig;
import org.junit.Test;

import java.io.IOException;

public class GarbageCollectorLowCacheTest extends EnvironmentTestsBase {

    @Override
    protected void createEnvironment() {
        env = newEnvironmentInstance(LogConfig.create(reader, writer), new EnvironmentConfig().setMemoryUsage(1).setMemoryUsagePercentage(0));
    }

    @Test
    public void collectExpiredPageWithKeyAddressPointingToDeletedFile() throws IOException, InterruptedException {
        /**
         * o. low cache, small file size
         * 1. create tree IP->BP(N1),BP(N2)
         * 2. save a lot of updates to last key of BP(N2),
         *    so ther're a lot of files with expired version of BP(N2) and
         *    links to min key of BP(N2), that was saved in a very first file
         * 3. clean first file, with min key of BP(N2)
         * 4. clean second file with expired version of BP(N2) and link to min key in removed file 
         */

        printDiskUsage();

        set1KbFileWithoutGC();
        env.getEnvironmentConfig().setTreeMaxPageSize(16);
        env.getEnvironmentConfig().setMemoryUsage(0);
        reopenEnvironment();

        Store store = openStoreAutoCommit("duplicates", getConfig());

        putAutoCommit(store, IntegerBinding.intToEntry(1), StringBinding.stringToEntry("value1"));
        putAutoCommit(store, IntegerBinding.intToEntry(2), StringBinding.stringToEntry("value2"));
        putAutoCommit(store, IntegerBinding.intToEntry(3), StringBinding.stringToEntry("value3"));
        putAutoCommit(store, IntegerBinding.intToEntry(4), StringBinding.stringToEntry("value4"));
        putAutoCommit(store, IntegerBinding.intToEntry(5), StringBinding.stringToEntry("value5"));
        putAutoCommit(store, IntegerBinding.intToEntry(6), StringBinding.stringToEntry("value6"));

        for (int i = 0; i < 1000; ++i) {
            putAutoCommit(store, IntegerBinding.intToEntry(6), StringBinding.stringToEntry("value6"));
        }

        final Log log = getLog();
        final GarbageCollector gc = getEnvironment().getGC();

        final long highFileAddress = log.getHighFileAddress();
        long fileAddress = log.getLowAddress();
        while (fileAddress != highFileAddress) {
            gc.doCleanFile(fileAddress);
            fileAddress = log.getNextFileAddress(fileAddress);
            gc.testDeletePendingFiles();
        }

        printDiskUsage();
    }

    protected StoreConfig getConfig() {
        return StoreConfig.WITHOUT_DUPLICATES;
    }

    private void printDiskUsage() throws IOException, InterruptedException {
        /*final Process spawned = Runtime.getRuntime().exec("df");
        Thread err = ForkSupport.createSpinner(spawned.getErrorStream(), System.err, 1024, "I/O [err]");
        Thread out = ForkSupport.createSpinner(spawned.getInputStream(), System.out, 1024, "I/O [out]");

        err.join();
        out.join();*/
    }

}
