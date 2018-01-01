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
package jetbrains.exodus.benchmark.lmdb;

import jetbrains.exodus.benchmark.TokyoCabinetBenchmark;
import org.fusesource.lmdbjni.*;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.IOException;

abstract class JMH_LMDBTokyoCabinetBenchmarkBase {

    private static final int MAP_SIZE = 1024 * 1024 * 1024;
    private static final byte[][] successiveKeys = TokyoCabinetBenchmark.getSuccessiveByteArrays(TokyoCabinetBenchmark.KEYS_COUNT);
    static final byte[][] randomKeys = TokyoCabinetBenchmark.getRandomByteArrays(TokyoCabinetBenchmark.KEYS_COUNT);

    private TemporaryFolder temporaryFolder;
    Env env;
    Database db;

    public void setup() throws IOException {
        TokyoCabinetBenchmark.shuffleKeys(randomKeys);
        temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        final File testsDirectory = temporaryFolder.newFolder("data");
        env = new Env();
        env.open(testsDirectory.getPath(), Constants.NOSYNC | Constants.WRITEMAP);
        env.setMapSize(MAP_SIZE);
        db = env.openDatabase();
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws IOException {
        if (db != null) {
            db.close();
            db = null;
        }
        if (env != null) {
            env.close();
            env = null;
        }
        if (temporaryFolder != null) {
            temporaryFolder.delete();
        }
    }

    void writeSuccessiveKeys() {
        try (Transaction txn = env.createWriteTransaction()) {
            try (BufferCursor c = db.bufferCursor(txn)) {
                for (final byte[] key : successiveKeys) {
                    c.keyWriteBytes(key);
                    c.valWriteBytes(key);
                    c.put();
                }
            }
            txn.commit();
        }
    }
}
