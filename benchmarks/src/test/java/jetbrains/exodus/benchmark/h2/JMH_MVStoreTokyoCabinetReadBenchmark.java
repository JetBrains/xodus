package jetbrains.exodus.benchmark.h2;

import org.h2.mvstore.MVStore;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.*;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMH_MVStoreTokyoCabinetReadBenchmark extends JMH_MVStoreTokyoCabinetBenchmarkBase {

    @Setup(Level.Invocation)
    public void beforeBenchmark() {
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
    public int successiveRead() {
        return computeInTransaction(new TransactionalComputable<Integer>() {
            @Override
            public Integer compute(@NotNull final MVStore store) {
                int result = 0;
                final Map<Object, Object> map = createTestMap(store);
                for (Map.Entry entry : map.entrySet()) {
                    result += ((String) entry.getKey()).length();
                    result += ((String) entry.getValue()).length();
                }
                return result;
            }
        });
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = 2)
    @Measurement(iterations = 4)
    @Fork(4)
    public int randomRead() {
        return computeInTransaction(new TransactionalComputable<Integer>() {
            @Override
            public Integer compute(@NotNull final MVStore store) {
                int result = 0;
                final Map<Object, Object> map = createTestMap(store);
                for (final String key : randomKeys) {
                    result += key.length();
                    result += ((String) map.get(key)).length();
                }
                return result;
            }
        });
    }
}

