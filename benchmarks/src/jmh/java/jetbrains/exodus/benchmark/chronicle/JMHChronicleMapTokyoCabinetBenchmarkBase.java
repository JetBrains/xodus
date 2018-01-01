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
package jetbrains.exodus.benchmark.chronicle;

import jetbrains.exodus.benchmark.TokyoCabinetBenchmark;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.VanillaChronicleMap;
import org.jetbrains.annotations.NotNull;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.util.Map;

abstract class JMHChronicleMapTokyoCabinetBenchmarkBase {

    private static final String[] successiveKeys = TokyoCabinetBenchmark.getSuccessiveStrings(TokyoCabinetBenchmark.KEYS_COUNT);
    static final String[] randomKeys = TokyoCabinetBenchmark.getRandomStrings(TokyoCabinetBenchmark.KEYS_COUNT);

    private TemporaryFolder temporaryFolder;
    private ChronicleMap<String, String> map;

    public void setup() throws IOException {
        TokyoCabinetBenchmark.shuffleKeys(randomKeys);
        createEnvironment();
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws IOException {
        closeTxMaker();
    }

    void writeSuccessiveKeys(@NotNull final Map<String, String> store) {
        for (final String key : successiveKeys) {
            store.put(key, key);
        }
    }

    private void createEnvironment() throws IOException {
        closeTxMaker();
        temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        map = ChronicleMap.of(String.class, String.class)
                .averageKeySize(8).averageValueSize(8)
                .entries(randomKeys.length)
                .createPersistedTo(temporaryFolder.newFile("data"));
    }

    private void closeTxMaker() {
        if (map != null) {
            map.close();
            map = null;
        }
        if (temporaryFolder != null) {
            temporaryFolder.delete();
        }
    }

    protected interface TransactionalExecutable {

        void execute(@NotNull final ChronicleMap<String, String> map);
    }

    void executeInTransaction(@NotNull final TransactionalExecutable executable) {
        executable.execute(map);
        try {
            ((VanillaChronicleMap) map).msync();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
