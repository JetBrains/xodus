package jetbrains.exodus.benchmark.h2;

import org.h2.mvstore.MVStore;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMH_MVStoreTokyoCabinetWriteBenchmark extends JMH_MVStoreTokyoCabinetBenchmarkBase {

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 2)
    @Measurement(iterations = 4)
    @Fork(4)
    public void successiveWrite() {
        computeInTransaction(new TransactionalComputable() {
            @Override
            public Object compute(@NotNull final MVStore store) {
                writeSuccessiveKeys(createTestMap(store));
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
            public Object compute(@NotNull final MVStore store) {
                final Map<Object, Object> map = createTestMap(store);
                for (final String key : randomKeys) {
                    map.put(key, key);
                }
                return null;
            }
        });
    }
}

