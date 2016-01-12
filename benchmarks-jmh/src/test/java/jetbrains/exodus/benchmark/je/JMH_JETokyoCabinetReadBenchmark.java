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
package jetbrains.exodus.benchmark.je;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMH_JETokyoCabinetReadBenchmark extends JMH_JETokyoCabinetBenchmarkBase {

    @Setup(Level.Invocation)
    public void beforeBenchmark() {
        writeSuccessiveKeys();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 2)
    @Measurement(iterations = 6)
    @Fork(10)
    public int successiveRead() {
        return computeInTransaction(new TransactionalComputable<Integer>() {
            @Override
            public Integer compute(@NotNull final Transaction txn) {
                int result = 0;
                try (Cursor c = store.openCursor(txn, null)) {
                    final DatabaseEntry key = new DatabaseEntry();
                    final DatabaseEntry value = new DatabaseEntry();
                    while (c.getNext(key, value, null) == OperationStatus.SUCCESS) {
                        result += key.getData().length;
                        result += value.getData().length;
                    }
                }
                return result;
            }
        });
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 2)
    @Measurement(iterations = 6)
    @Fork(10)
    public int randomRead() {
        return computeInTransaction(new TransactionalComputable<Integer>() {
            @Override
            public Integer compute(@NotNull final Transaction txn) {
                int result = 0;
                try (Cursor c = store.openCursor(txn, null)) {
                    for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
                        final DatabaseEntry key = new DatabaseEntry(randomKeys[i].getData());
                        final DatabaseEntry value = new DatabaseEntry();
                        c.getSearchKey(key, value, null);
                        result += key.getData().length;
                        result += value.getData().length;
                    }
                }
                return result;
            }
        });
    }

    @Override
    protected boolean isKeyPrefixing() {
        return false;
    }
}
