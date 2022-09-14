package jetbrains.exodus.benchmark.env.tokyo;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ByteIterator;
import jetbrains.exodus.env.StoreConfig;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static jetbrains.exodus.benchmark.TokyoCabinetBenchmark.*;
import static jetbrains.exodus.benchmark.TokyoCabinetBenchmark.FORKS;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMHEnvTokyoCabinetReadInlineBenchmark extends JMHEnvTokyoCabinetBenchmarkBase {
    @Setup(Level.Invocation)
    public void beforeBenchmark() throws IOException {
        setup();
        writeSuccessiveKeys();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = WARMUP_ITERATIONS)
    @Measurement(iterations = MEASUREMENT_ITERATIONS)
    @Fork(FORKS)
    public void successiveRead(final Blackhole bh) {
        for (var key : successiveKeys) {
            env.executeInReadonlyTransaction(txn -> {
                var value = store.get(txn, key);
                consumeBytes(bh, value);
            });
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = WARMUP_ITERATIONS)
    @Measurement(iterations = MEASUREMENT_ITERATIONS)
    @Fork(FORKS)
    public void randomRead(final Blackhole bh) {
        for (var key : randomKeys) {
            env.executeInReadonlyTransaction(txn -> {
                var value = store.get(txn, key);
                bh.consume(value);
            });
        }
    }

    @Override
    protected StoreConfig getStoreConfig() {
        return StoreConfig.WITHOUT_DUPLICATES_INLINE;
    }

    private static void consumeBytes(final Blackhole bh, final ByteIterable it) {
        final ByteIterator iterator = it.iterator();
        while (iterator.hasNext()) {
            bh.consume(iterator.next());
        }
    }
}
