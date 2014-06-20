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
public class JMH_JETokyoCabinetLikeReadBenchmark extends JMH_JETokyoCabinetLikeBenchmarkBase {

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
        return computeInTransaction(new TransactionalComputable<Integer>() {
            @Override
            public Integer compute(@NotNull final Transaction txn) {
                int result = 0;
                final Cursor c = store.openCursor(txn, null);
                try {
                    final DatabaseEntry key = new DatabaseEntry();
                    final DatabaseEntry value = new DatabaseEntry();
                    while (c.getNext(key, value, null) == OperationStatus.SUCCESS) {
                        result += key.getData().length;
                        result += value.getData().length;
                    }
                } finally {
                    c.close();
                }
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
        return computeInTransaction(new TransactionalComputable<Integer>() {
            @Override
            public Integer compute(@NotNull final Transaction txn) {
                int result = 0;
                final Cursor c = store.openCursor(txn, null);
                try {
                    for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
                        final DatabaseEntry key = new DatabaseEntry(randomKeys[i].getData());
                        final DatabaseEntry value = new DatabaseEntry();
                        c.getSearchKey(key, value, null);
                        result += key.getData().length;
                        result += value.getData().length;
                    }
                } finally {
                    c.close();
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
