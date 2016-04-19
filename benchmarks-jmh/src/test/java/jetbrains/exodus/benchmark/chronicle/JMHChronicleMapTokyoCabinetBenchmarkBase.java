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
package jetbrains.exodus.benchmark.chronicle;

import jetbrains.exodus.benchmark.BenchmarkTestBase;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.VanillaChronicleMap;
import org.jetbrains.annotations.NotNull;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public abstract class JMHChronicleMapTokyoCabinetBenchmarkBase extends BenchmarkTestBase {

    protected static final String[] successiveKeys;
    protected static final String[] randomKeys;

    public static final String PATTERN = "00000000";

    static {
        final DecimalFormat FORMAT = (DecimalFormat) NumberFormat.getIntegerInstance();
        FORMAT.applyPattern(PATTERN);
        successiveKeys = new String[TOKYO_CABINET_BENCHMARK_SIZE];
        for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
            successiveKeys[i] = FORMAT.format(i);
        }
        randomKeys = Arrays.copyOf(successiveKeys, successiveKeys.length);
        shuffleKeys();
    }

    private ChronicleMap<String, String> map;

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

    protected void writeSuccessiveKeys(@NotNull final Map<String, String> store) {
        for (final String key : successiveKeys) {
            store.put(key, key);
        }
    }

    private void createEnvironment() throws IOException {
        closeTxMaker();
        map = ChronicleMap.of(String.class, String.class)
                .averageKey(PATTERN).averageValue(PATTERN).entries(randomKeys.length)
                .createPersistedTo(new File("data"));
    }

    private void closeTxMaker() {
        if (map != null) {
            map.close();
            map = null;
        }
    }

    protected static void shuffleKeys() {
        Collections.shuffle(Arrays.asList(randomKeys));
    }

    protected interface TransactionalComputable<T> {

        T compute(@NotNull final ChronicleMap<String, String> map);
    }

    protected <T> T computeInTransaction(@NotNull final TransactionalComputable<T> computable) {
        T result = computable.compute(map);
        try {
            ((VanillaChronicleMap) map).msync();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}
