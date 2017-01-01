/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
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

import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static jetbrains.exodus.benchmark.TokyoCabinetBenchmark.MEASUREMENT_ITERATIONS;
import static jetbrains.exodus.benchmark.TokyoCabinetBenchmark.WARMUP_ITERATIONS;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMHMapDbTokyoCabinetReadBenchmark extends JMHMapDbTokyoCabinetBenchmarkBase {

    @Setup(Level.Invocation)
    public void beforeBenchmark() throws IOException {
        setup();
        writeSuccessiveKeys(createTestStore());
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = WARMUP_ITERATIONS)
    @Measurement(iterations = MEASUREMENT_ITERATIONS)
    @Fork(4)
    public int successiveRead() {
        int result = 0;
        final Map<String, String> store = createTestStore();
        for (Map.Entry<String, String> entry : store.entrySet()) {
            result += entry.getKey().length();
            result += entry.getValue().length();
        }
        return result;
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = WARMUP_ITERATIONS)
    @Measurement(iterations = MEASUREMENT_ITERATIONS)
    @Fork(4)
    public int randomRead() {
        int result = 0;
        final Map<String, String> store = createTestStore();
        for (final String key : randomKeys) {
            result += key.length();
            result += store.get(key).length();
        }
        return result;
    }
}
