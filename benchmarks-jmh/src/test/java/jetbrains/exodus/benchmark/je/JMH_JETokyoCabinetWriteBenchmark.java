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

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMH_JETokyoCabinetWriteBenchmark extends JMH_JETokyoCabinetBenchmarkBase {

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 2)
    @Measurement(iterations = 6)
    @Fork(10)
    public void successiveWrite() {
        writeSuccessiveKeys();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 2)
    @Measurement(iterations = 6)
    @Fork(10)
    public void randomWrite() {
        computeInTransaction(new TransactionalComputable<Object>() {
            @Override
            public Object compute(@NotNull Transaction txn) {
                for (final DatabaseEntry key : randomKeys) {
                    store.put(txn, key, key);
                }
                return null;
            }
        });
    }

    @Override
    protected boolean isKeyPrefixing() {
        return false;
    }
}
