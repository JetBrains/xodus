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

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ExternalMapQueryContext;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static jetbrains.exodus.benchmark.TokyoCabinetBenchmark.*;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMHChronicleMapTokyoCabinetReadBenchmark extends JMHChronicleMapTokyoCabinetBenchmarkBase {

    @Setup(Level.Invocation)
    public void beforeBenchmark() throws IOException {
        setup();
        executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull ChronicleMap<String, String> map) {
                writeSuccessiveKeys(map);
            }
        });
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = WARMUP_ITERATIONS)
    @Measurement(iterations = MEASUREMENT_ITERATIONS)
    @Fork(FORKS)
    public void successiveRead(final Blackhole bh) {
        executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull ChronicleMap<String, String> map) {
                map.forEachEntry(e -> {
                    bh.consume(e.key().get());
                    bh.consume(e.value().get());
                });
            }
        });
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = WARMUP_ITERATIONS)
    @Measurement(iterations = MEASUREMENT_ITERATIONS)
    @Fork(FORKS)
    public void randomRead(final Blackhole bh) {
        executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull ChronicleMap<String, String> map) {
                for (final String key : randomKeys) {
                    try (ExternalMapQueryContext<String, String, ?> c = map.queryContext(key)) {
                        bh.consume(c.entry().value().get());
                    }
                }
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
