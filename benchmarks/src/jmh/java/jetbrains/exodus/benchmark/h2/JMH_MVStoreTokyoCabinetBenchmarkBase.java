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
package jetbrains.exodus.benchmark.h2;

import jetbrains.exodus.benchmark.TokyoCabinetBenchmark;
import org.h2.mvstore.MVStore;
import org.jetbrains.annotations.NotNull;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.util.Map;

abstract class JMH_MVStoreTokyoCabinetBenchmarkBase {

    private static final String[] successiveKeys = TokyoCabinetBenchmark.getSuccessiveStrings(TokyoCabinetBenchmark.KEYS_COUNT);
    static final String[] randomKeys = TokyoCabinetBenchmark.getRandomStrings(TokyoCabinetBenchmark.KEYS_COUNT);

    private TemporaryFolder temporaryFolder;
    private MVStore store;

    public void setup() throws IOException {
        TokyoCabinetBenchmark.shuffleKeys(randomKeys);
        createEnvironment();
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws IOException {
        closeStore();
    }

    void writeSuccessiveKeys(@NotNull final Map<Object, Object> map) {
        for (final String key : successiveKeys) {
            map.put(key, key);
        }
    }

    Map<Object, Object> createTestMap(@NotNull final MVStore store) {
        return store.openMap("testTokyoCabinet");
    }

    private void createEnvironment() throws IOException {
        closeStore();
        temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        store = new MVStore.Builder()
            .fileName(temporaryFolder.newFile("data").getAbsolutePath())
            .autoCommitDisabled()
            .open();
    }

    private void closeStore() {
        if (store != null) {
            store.close();
            store = null;
        }
        if (temporaryFolder != null) {
            temporaryFolder.delete();
        }
    }

    protected interface TransactionalExecutable {

        void execute(@NotNull final MVStore store);
    }

    void executeInTransaction(@NotNull final TransactionalExecutable executable) {
        try {
            executable.execute(store);
        } finally {
            store.commit();
        }
    }
}

