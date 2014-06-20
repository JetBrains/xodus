package jetbrains.exodus.benchmark.je;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMH_JETokyoCabinetLikeWriteBenchmark extends JMH_JETokyoCabinetLikeBenchmarkBase {

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
