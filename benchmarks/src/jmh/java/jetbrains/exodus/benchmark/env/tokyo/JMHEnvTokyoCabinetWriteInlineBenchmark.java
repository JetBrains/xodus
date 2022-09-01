package jetbrains.exodus.benchmark.env.tokyo;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.env.StoreConfig;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static jetbrains.exodus.benchmark.TokyoCabinetBenchmark.*;
import static jetbrains.exodus.benchmark.TokyoCabinetBenchmark.FORKS;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JMHEnvTokyoCabinetWriteInlineBenchmark extends JMHEnvTokyoCabinetBenchmarkByteBufferBase {

    @Setup(Level.Invocation)
    public void beforeBenchmark() throws IOException {
        setup();
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = WARMUP_ITERATIONS)
    @Measurement(iterations = MEASUREMENT_ITERATIONS)
    @Fork(FORKS)
    public void successiveWrite() {
        for (final ByteBuffer key : successiveKeys) {
            env.executeInTransaction(txn -> {
                store.add(txn, key, key);
            });
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Warmup(iterations = WARMUP_ITERATIONS)
    @Measurement(iterations = MEASUREMENT_ITERATIONS)
    @Fork(FORKS)
    public void randomWrite() {
        for (final ByteBuffer key : randomKeys) {
            env.executeInTransaction(txn -> {
                store.add(txn, key, key);
            });
        }
    }

    @Override
    protected StoreConfig getStoreConfig() {
        return StoreConfig.WITHOUT_DUPLICATES_INLINE;
    }
}
