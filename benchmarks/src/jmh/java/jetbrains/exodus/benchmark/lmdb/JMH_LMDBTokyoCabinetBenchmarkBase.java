package jetbrains.exodus.benchmark.lmdb;

import jetbrains.exodus.benchmark.TokyoCabinetBenchmark;
import org.fusesource.lmdbjni.*;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.IOException;

abstract class JMH_LMDBTokyoCabinetBenchmarkBase {

    private static final int MAP_SIZE = 1024 * 1024 * 1024;
    private static final byte[][] successiveKeys = TokyoCabinetBenchmark.getSuccessiveByteArrays(TokyoCabinetBenchmark.KEYS_COUNT);
    static final byte[][] randomKeys = TokyoCabinetBenchmark.getRandomByteArrays(TokyoCabinetBenchmark.KEYS_COUNT);

    Env env;
    Database db;

    public void setup() throws IOException {
        TokyoCabinetBenchmark.shuffleKeys(randomKeys);
        final TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        final File testsDirectory = temporaryFolder.newFolder("data");
        env = new Env(testsDirectory.getPath());
        env.setMapSize(MAP_SIZE);
        env.addFlags(Constants.NOSYNC);
        db = env.openDatabase();
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws IOException {
        if (db != null) {
            db.close();
            db = null;
        }
        if (env != null) {
            env.close();
            env = null;
        }
    }

    void writeSuccessiveKeys() {
        try (Transaction txn = env.createWriteTransaction()) {
            try (BufferCursor c = db.bufferCursor(txn)) {
                for (final byte[] key : successiveKeys) {
                    c.keyWriteBytes(key);
                    c.valWriteBytes(key);
                    c.put();
                }
            }
            txn.commit();
        }
    }
}
