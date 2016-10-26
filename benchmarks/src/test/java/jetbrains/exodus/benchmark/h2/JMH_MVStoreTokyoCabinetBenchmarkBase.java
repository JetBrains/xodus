package jetbrains.exodus.benchmark.h2;

import jetbrains.exodus.benchmark.TokyoCabinetBenchmark;
import org.h2.mvstore.MVStore;
import org.jetbrains.annotations.NotNull;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.util.Map;

abstract class JMH_MVStoreTokyoCabinetBenchmarkBase {

    private static final String[] successiveKeys = TokyoCabinetBenchmark.getSuccessiveStrings(TokyoCabinetBenchmark.KEYS_COUNT);
    static final String[] randomKeys = TokyoCabinetBenchmark.getRandomStrings(TokyoCabinetBenchmark.KEYS_COUNT);

    private MVStore store;

    @Setup(Level.Invocation)
    public void setup() throws IOException {
        TokyoCabinetBenchmark.shuffleKeys(randomKeys);
        createEnvironment();
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws IOException {
        closeStore();
    }

    void writeSuccessiveKeys(@NotNull final Map<Object, Object> map) {
        for (final String key : successiveKeys) {
            map.put(key, key);
        }
    }

    Map<Object, Object> createTestMap(@NotNull final MVStore store) {
        return store.openMap("testTokyoCabinet");
    }

    private void createEnvironment() throws IOException {
        closeStore();
        final TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        store = new MVStore.Builder()
                .fileName(temporaryFolder.newFile("data").getAbsolutePath())
                .autoCommitDisabled()
                .open();
    }

    private void closeStore() {
        if (store != null) {
            store.close();
            store = null;
        }
    }

    protected interface TransactionalComputable<T> {

        T compute(@NotNull final MVStore store);
    }

    <T> T computeInTransaction(@NotNull final TransactionalComputable<T> computable) {
        try {
            return computable.compute(store);
        } finally {
            store.commit();
        }
    }
}

