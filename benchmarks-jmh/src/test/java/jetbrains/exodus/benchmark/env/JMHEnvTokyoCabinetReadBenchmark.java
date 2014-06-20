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
    @Warmup(iterations = 4)
    @Measurement(iterations = 8)
    @Fork(4)
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
    @Warmup(iterations = 4)
    @Measurement(iterations = 8)
    @Fork(4)
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
