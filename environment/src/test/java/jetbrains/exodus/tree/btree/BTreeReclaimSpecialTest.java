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
package jetbrains.exodus.tree.btree;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.log.LogConfig;
import jetbrains.exodus.log.LogUtil;
import jetbrains.exodus.log.NullLoggable;
import jetbrains.exodus.log.RandomAccessLoggable;
import jetbrains.exodus.tree.ITreeCursor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class BTreeReclaimSpecialTest extends BTreeTestBase {
    private static final int COUNT = 145;

    @Override
    protected LogConfig createLogConfig() {
        LogConfig result = super.createLogConfig();
        result.setFileSize(1);
        return result;
    }

    @Test
    public void testStartAddress() {
        final long fileSize = log.getFileSize() * LogUtil.LOG_BLOCK_ALIGNMENT;
        for (long l = 1; l < fileSize; ++l) { // fill all file except for one byte with nulls
            log.write(NullLoggable.create());
        }
        Assert.assertEquals(1, log.getNumberOfFiles());
        Assert.assertTrue(log.getHighAddress() < fileSize);
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();
        final ArrayByteIterable key = key("K");
        for (int i = 0; i <= COUNT; i++) {
            tm.put(key, v(i));
        }
        reloadMutableTree(tm.save());
        Assert.assertEquals(4, log.getNumberOfFiles());
        log.removeFile(0); // emulate gc of first file
        Iterator<RandomAccessLoggable> loggables = log.getLoggableIterator(log.getFileAddress(fileSize * 2));
        tm.reclaim(loggables.next(), loggables); // reclaim third file
        reloadMutableTree(tm.save());
        log.removeFile(fileSize * 2); // remove reclaimed file
        loggables = log.getLoggableIterator(log.getFileAddress(fileSize));
        tm.reclaim(loggables.next(), loggables); // reclaim second file
        reloadMutableTree(tm.save());
        Assert.assertTrue(log.getNumberOfFiles() > 2); // make sure that some files were added
        log.removeFile(fileSize); // remove reclaimed file
        try (ITreeCursor cursor = tm.openCursor()) {
            Assert.assertTrue(cursor.getNext()); // access minimum key
        }
    }

    @Test
    public void testDups() {
        tm = new BTreeEmpty(log, true, 1).getMutableCopy();
        tm.put(key("k"), value("v0"));
        tm.put(key("k"), value("v1"));
        long firstAddress = tm.save();
        reloadMutableTree(firstAddress);
        tm.put(key("k"), value("v2"));
        tm.put(key("k"), value("v3"));
        tm.save();
        Iterator<RandomAccessLoggable> loggables = log.getLoggableIterator(0);
        tm.reclaim(loggables.next(), loggables);
        loggables = log.getLoggableIterator(firstAddress);
        loggables.next();
        tm.reclaim(loggables.next(), loggables);
    }

    private void reloadMutableTree(long address) {
        tm = new BTree(log, getTreeMutable().getBalancePolicy(), address, true, 1).getMutableCopy();
    }
}
