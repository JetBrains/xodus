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

import jetbrains.exodus.OutOfDiskSpaceException;
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.io.DataReader;
import jetbrains.exodus.io.DataWriter;
import jetbrains.exodus.io.inMemory.Memory;
import jetbrains.exodus.io.inMemory.MemoryDataReader;
import jetbrains.exodus.io.inMemory.MemoryDataWriter;
import jetbrains.exodus.tree.btree.BTreeBase;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.IOException;

public class OutOfDiskSpaceTest extends EnvironmentTestsBase {
    private int[] SEQ;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        env.getEnvironmentConfig().setGcEnabled(false);
        SEQ = new int[]{BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE,
                BTreeBase.BOTTOM_ROOT, BTreeBase.LEAF, BTreeBase.LEAF, BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE,
                BTreeBase.BOTTOM_ROOT, BTreeBase.LEAF, BTreeBase.LEAF, BTreeBase.BOTTOM_ROOT, DatabaseRoot.DATABASE_ROOT_TYPE};
    }

    @Override
    protected Pair<DataReader, DataWriter> createRW() throws IOException {
        Memory memory = new Memory();
        return new Pair<DataReader, DataWriter>(new MemoryDataReader(memory), new LimitedMemoryDataWriter(memory));
    }

    @Override
    protected void deleteRW() {
        reader = null;
        writer = null;
    }

    @Test
    public void testRecovery() throws IOException {
        Transaction txn = env.beginTransaction();
        env.openStore("new_store", StoreConfig.WITHOUT_DUPLICATES, txn);
        txn.commit();
        ((LimitedMemoryDataWriter) writer).limit = 15;
        TestUtil.runWithExpectedException(new Runnable() {
            @Override
            public void run() {
                createAnotherStore();
            }
        }, OutOfDiskSpaceException.class);
        ((LimitedMemoryDataWriter) writer).limit = -1;
        createAnotherStore();
        assertLoggableTypes(getLog(), 0, SEQ);
    }

    private void createAnotherStore() {
        Transaction txn;
        txn = env.beginTransaction();
        try {
            env.openStore("another_store", StoreConfig.WITHOUT_DUPLICATES, txn);
            txn.flush();
            env.flushAndSync();
        } finally {
            txn.abort();
        }
    }

    private static final class LimitedMemoryDataWriter extends MemoryDataWriter {

        private long limit = -1;
        private long written = 0;

        private LimitedMemoryDataWriter(@NotNull Memory memory) {
            super(memory);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            if (limit >= 0 && written + len > limit) {
                throw new OutOfDiskSpaceException(new IOException("No space left on device"));
            }
            super.write(b, off, len);
            written += len;
        }
    }
}
