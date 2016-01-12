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
package jetbrains.exodus.benchmark.env;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.env.TransactionalComputable;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMHEnvTokyoCabinetReadBenchmark extends JMHEnvTokyoCabinetBenchmarkBase {

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
        return env.computeInReadonlyTransaction(new TransactionalComputable<Integer>() {
            @Override
            public Integer compute(@NotNull final Transaction txn) {
                int result = 0;
                final Cursor c = store.openCursor(txn);
                while (c.getNext()) {
                    result += c.getKey().getLength();
                    result += c.getValue().getLength();
                }
                c.close();
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
        return env.computeInReadonlyTransaction(new TransactionalComputable<Integer>() {
            @Override
            public Integer compute(@NotNull final Transaction txn) {
                int result = 0;
                final Cursor c = store.openCursor(txn);
                for (final ByteIterable key : randomKeys) {
                    c.getSearchKey(key);
                    result += c.getKey().getLength();
                    result += c.getValue().getLength();
                }
                c.close();
                return result;
            }
        });
    }

    @Override
    protected StoreConfig getConfig() {
        return StoreConfig.WITHOUT_DUPLICATES;
    }
}
