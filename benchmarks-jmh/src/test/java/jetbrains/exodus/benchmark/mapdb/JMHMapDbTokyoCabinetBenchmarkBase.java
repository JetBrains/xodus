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

import jetbrains.exodus.benchmark.BenchmarkTestBase;
import org.jetbrains.annotations.NotNull;
import org.junit.rules.TemporaryFolder;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TxMaker;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public abstract class JMHMapDbTokyoCabinetBenchmarkBase extends BenchmarkTestBase {

    protected static final String[] successiveKeys;
    protected static final String[] randomKeys;

    static {
        final DecimalFormat FORMAT = (DecimalFormat) NumberFormat.getIntegerInstance();
        FORMAT.applyPattern("00000000");
        successiveKeys = new String[TOKYO_CABINET_BENCHMARK_SIZE];
        for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
            successiveKeys[i] = FORMAT.format(i);
        }
        randomKeys = Arrays.copyOf(successiveKeys, successiveKeys.length);
        shuffleKeys();
    }

    private TxMaker txMaker;

    @Setup(Level.Invocation)
    public void setup() throws IOException {
        start();
        shuffleKeys();
        temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        createEnvironment();
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws IOException {
        closeTxMaker();
        end();
    }

    protected void writeSuccessiveKeys(@NotNull final Map<Object, Object> store) {
        for (final String key : successiveKeys) {
            store.put(key, key);
        }
    }

    protected Map<Object, Object> createTestStore(@NotNull final DB db) {
        return db.getTreeMap("testTokyoCabinet");
    }

    private void createEnvironment() throws IOException {
        closeTxMaker();
        txMaker = DBMaker.newFileDB(temporaryFolder.newFile("data")).makeTxMaker();
    }

    private void closeTxMaker() {
        if (txMaker != null) {
            txMaker.close();
            txMaker = null;
        }
    }

    protected static void shuffleKeys() {
        Collections.shuffle(Arrays.asList(randomKeys));
    }

    protected interface TransactionalComputable<T> {

        T compute(@NotNull final DB db);
    }

    protected <T> T computeInTransaction(@NotNull final TransactionalComputable<T> computable) {
        final DB db = txMaker.makeTx();
        try {
            return computable.compute(db);
        } finally {
            db.commit();
        }
    }
}
