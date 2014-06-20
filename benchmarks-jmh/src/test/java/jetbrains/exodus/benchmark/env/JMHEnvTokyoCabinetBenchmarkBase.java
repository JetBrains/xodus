package jetbrains.exodus.benchmark.env;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.benchmark.BenchmarkTestBase;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.*;
import jetbrains.exodus.io.FileDataReader;
import jetbrains.exodus.io.FileDataWriter;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Random;

abstract class JMHEnvTokyoCabinetBenchmarkBase extends BenchmarkTestBase {

    protected static final ByteIterable[] successiveKeys;
    protected static final ByteIterable[] randomKeys;

    static {
        final DecimalFormat FORMAT = (DecimalFormat) NumberFormat.getIntegerInstance();
        FORMAT.applyPattern("00000000");
        successiveKeys = new ByteIterable[TOKYO_CABINET_BENCHMARK_SIZE];
        for (int i = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; i++) {
            successiveKeys[i] = StringBinding.stringToEntry(FORMAT.format(i));
        }
        randomKeys = new ByteIterable[TOKYO_CABINET_BENCHMARK_SIZE];
        final int prime = BigInteger.probablePrime(19, new Random()).intValue();
        for (int i = 0, j = 0; i < TOKYO_CABINET_BENCHMARK_SIZE; ++i, j = (j + prime) % TOKYO_CABINET_BENCHMARK_SIZE) {
            randomKeys[j] = successiveKeys[i];
        }
    }

    private static final String STORE_NAME = "TokyoCabinetBenchmarkStore";

    protected Environment env;
    protected Store store;

    @Setup(Level.Invocation)
    public void setup() throws IOException {
        start();
        Log.invalidateSharedCache();
        final TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        final File testsDirectory = temporaryFolder.newFolder("data");
        LogConfig config = new LogConfig();
        config.setReader(new FileDataReader(testsDirectory, 16));
        config.setWriter(new FileDataWriter(testsDirectory));
        env = Environments.newInstance(config, new EnvironmentConfig());
        store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull Transaction txn) {
                return env.openStore(STORE_NAME, getConfig(), txn);
            }
        });
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws IOException {
        env.close();
        end();
    }

    protected void writeSuccessiveKeys() {
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                for (final ByteIterable key : successiveKeys) {
                    store.add(txn, key, key);
                }
            }
        });
    }

    protected abstract StoreConfig getConfig();
}
