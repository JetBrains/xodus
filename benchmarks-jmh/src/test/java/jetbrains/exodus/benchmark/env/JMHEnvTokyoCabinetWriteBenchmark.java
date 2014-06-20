package jetbrains.exodus.benchmark.env;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.env.TransactionalExecutable;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMHEnvTokyoCabinetWriteBenchmark extends JMHEnvTokyoCabinetBenchmarkBase {

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 4)
    @Measurement(iterations = 8)
    @Fork(4)
    public void successiveWrite() {
        writeSuccessiveKeys();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 4)
    @Measurement(iterations = 8)
    @Fork(4)
    public void randomWrite() {
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                for (final ByteIterable key : randomKeys) {
                    store.add(txn, key, key);
                }
            }
        });
    }

    @Override
    protected StoreConfig getConfig() {
        return StoreConfig.WITHOUT_DUPLICATES;
    }
}

