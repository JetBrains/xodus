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
package jetbrains.exodus.benchmark.mapdb;

import jetbrains.exodus.benchmark.TokyoCabinetBenchmark;
import org.jetbrains.annotations.NotNull;
import org.junit.rules.TemporaryFolder;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.util.Map;

abstract class JMHMapDbTokyoCabinetBenchmarkBase {

    private static final String[] successiveKeys = TokyoCabinetBenchmark.getSuccessiveStrings(TokyoCabinetBenchmark.KEYS_COUNT);
    static final String[] randomKeys = TokyoCabinetBenchmark.getRandomStrings(TokyoCabinetBenchmark.KEYS_COUNT);

    private TemporaryFolder temporaryFolder;
    DB db;
    private BTreeMap<String, String> map;

    public void setup() throws IOException {
        TokyoCabinetBenchmark.shuffleKeys(randomKeys);
        createEnvironment();
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws IOException {
        closeDb();
    }

    void writeSuccessiveKeys(@NotNull final Map<String, String> store) {
        for (final String key : successiveKeys) {
            store.put(key, key);
        }
        db.commit();
    }

    Map<String, String> createTestStore() {
        if (map == null) {
            map = db.treeMap("testTokyoCabinet").keySerializer(Serializer.STRING).valueSerializer(Serializer.STRING).createOrOpen();
        }
        return map;
    }

    private void createEnvironment() throws IOException {
        closeDb();
        temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        db = DBMaker.tempFileDB().fileMmapEnable().concurrencyDisable().make();
    }

    private void closeDb() {
        if (db != null) {
            map.close();
            map = null;
            db.close();
            db = null;
        }
        if (temporaryFolder != null) {
            temporaryFolder.delete();
        }
    }
}
