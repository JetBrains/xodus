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

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.openjdk.jmh.annotations.*;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMHMapDbTokyoCabinetWriteBenchmark extends JMHMapDbTokyoCabinetBenchmarkBase {

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 2)
    @Measurement(iterations = 4)
    @Fork(4)
    public void successiveWrite() {
        computeInTransaction(new TransactionalComputable() {
            @Override
            public Object compute(@NotNull final DB db) {
                writeSuccessiveKeys(createTestStore(db));
                return null;
            }
        });
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 2)
    @Measurement(iterations = 4)
    @Fork(4)
    public void randomWrite() {
        computeInTransaction(new TransactionalComputable() {
            @Override
            public Object compute(@NotNull final DB db) {
                final Map<Object, Object> store = createTestStore(db);
                for (final String key : randomKeys) {
                    store.put(key, key);
                }
                return null;
            }
        });
    }
}
