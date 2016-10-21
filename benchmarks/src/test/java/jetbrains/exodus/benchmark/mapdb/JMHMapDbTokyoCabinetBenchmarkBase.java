/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.benchmark.mapdb;

import jetbrains.exodus.benchmark.TokyoCabinetBenchmark;
import org.jetbrains.annotations.NotNull;
import org.junit.rules.TemporaryFolder;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TxMaker;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.util.Map;

abstract class JMHMapDbTokyoCabinetBenchmarkBase {

    private static final String[] successiveKeys = TokyoCabinetBenchmark.getSuccessiveStrings(TokyoCabinetBenchmark.KEYS_COUNT);
    static final String[] randomKeys = TokyoCabinetBenchmark.getRandomStrings(TokyoCabinetBenchmark.KEYS_COUNT);

    private TxMaker txMaker;

    @Setup(Level.Invocation)
    public void setup() throws IOException {
        TokyoCabinetBenchmark.shuffleKeys(randomKeys);
        createEnvironment();
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws IOException {
        closeTxMaker();
    }

    void writeSuccessiveKeys(@NotNull final Map<Object, Object> store) {
        for (final String key : successiveKeys) {
            store.put(key, key);
        }
    }

    Map<Object, Object> createTestStore(@NotNull final DB db) {
        return db.getTreeMap("testTokyoCabinet");
    }

    private void createEnvironment() throws IOException {
        closeTxMaker();
        final TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        txMaker = DBMaker.newFileDB(temporaryFolder.newFile("data")).makeTxMaker();
    }

    private void closeTxMaker() {
        if (txMaker != null) {
            txMaker.close();
            txMaker = null;
        }
    }

    protected interface TransactionalComputable<T> {

        T compute(@NotNull final DB db);
    }

    <T> T computeInTransaction(@NotNull final TransactionalComputable<T> computable) {
        final DB db = txMaker.makeTx();
        try {
            return computable.compute(db);
        } finally {
            db.commit();
        }
    }
}
