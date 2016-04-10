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

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMHChronicleMapTokyoCabinetReadBenchmark extends JMHChronicleMapTokyoCabinetBenchmarkBase {

    @Setup(Level.Invocation)
    public void beforeBenchmark() {
        computeInTransaction(new TransactionalComputable<Void>() {
            @Override
            public Void compute(@NotNull ChronicleMap<String, String> map) {
               writeSuccessiveKeys(map);
                return null;
            }
        });
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 2)
    @Measurement(iterations = 4)
    @Fork(4)
    public int successiveRead() {
        return computeInTransaction(new TransactionalComputable<Integer>() {
            int result;
            @Override
            public Integer compute(@NotNull ChronicleMap<String, String> map) {
                result = 0;
                map.forEachEntry(e -> {
                    result += e.key().size();
                    result += e.value().size();
                });
                return result;
            }
        });
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 2)
    @Measurement(iterations = 4)
    @Fork(4)
    public int randomRead() {
        return computeInTransaction(new TransactionalComputable<Integer>() {
            @Override
            public Integer compute(@NotNull ChronicleMap<String, String> map) {
                int result = 0;
                for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; ++i) {
                    final String key = randomKeys[i];
                    result += key.length();
                    try (ExternalMapQueryContext<String, String, ?> c = map.queryContext(key)) {
                        result += c.entry().value().size();
                    }
                }
                return result;
            }
        });
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(JMHChronicleMapTokyoCabinetReadBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }
}
