/**
 * Copyright 2010 - 2018 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.env;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.TestUtil;
import jetbrains.exodus.backup.BackupStrategy;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.core.execution.JobProcessor;
import jetbrains.exodus.core.execution.ThreadJobProcessor;
import jetbrains.exodus.core.execution.locks.Latch;
import jetbrains.exodus.io.*;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogConfig;
import jetbrains.exodus.log.RandomAccessLoggable;
import jetbrains.exodus.util.CompressBackupUtil;
import jetbrains.exodus.util.IOUtil;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.zip.GZIPOutputStream;

@SuppressWarnings({"ProtectedField", "UnusedDeclaration", "CallToPrintStackTrace"})
public class EnvironmentTestsBase {

    protected EnvironmentImpl env;
    private JobProcessor processor;
    private File envDirectory = null;
    protected DataReader reader;
    protected DataWriter writer;

    @Before
    public void setUp() throws Exception {
        invalidateSharedCaches();
        final Pair<DataReader, DataWriter> readerWriterPair = createRW();
        reader = readerWriterPair.getFirst();
        writer = readerWriterPair.getSecond();
        createEnvironment();
        processor = new ThreadJobProcessor("EnvironmentTestsBase processor");
        processor.start();
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (env != null) {
                env.close();
                env = null;
            }
        } catch (final ExodusException e) {
            archiveDB(env.getLocation(), getClass().getName() + '.' + System.currentTimeMillis() + ".tar.gz");
            throw e;
        } finally {
            invalidateSharedCaches();
            deleteRW();
            processor.finish();
        }
    }

    public static void archiveDB(final String location, final String target) {
        try {
            System.out.println("Dumping " + location + " to " + target);
            final File root = new File(location);
            final File targetFile = new File(target);
            TarArchiveOutputStream tarGz = new TarArchiveOutputStream(new GZIPOutputStream(
                new BufferedOutputStream(new FileOutputStream(targetFile)), 0x1000));
            for (final File file : IOUtil.listFiles(root)) {
                final long fileSize = file.length();
                if (file.isFile() && fileSize != 0) {
                    CompressBackupUtil.archiveFile(tarGz, new BackupStrategy.FileDescriptor(file, ""), fileSize);
                }
            }
            tarGz.close();
        } catch (IOException ioe) {
            System.out.println("Can't create backup");
        }
    }

    protected EnvironmentImpl getEnvironment() {
        return env;
    }

    protected EnvironmentImpl newEnvironmentInstance(final LogConfig config) {
        return (EnvironmentImpl) Environments.newInstance(config);
    }

    protected EnvironmentImpl newEnvironmentInstance(final LogConfig config, final EnvironmentConfig ec) {
        return (EnvironmentImpl) Environments.newInstance(config, ec);
    }

    protected EnvironmentImpl newContextualEnvironmentInstance(final LogConfig config) {
        return newContextualEnvironmentInstance(config, new EnvironmentConfig());
    }

    protected EnvironmentImpl newContextualEnvironmentInstance(final LogConfig config, final EnvironmentConfig ec) {
        return (EnvironmentImpl) Environments.newContextualInstance(config, ec);
    }

    protected void createEnvironment() {
        env = newEnvironmentInstance(LogConfig.create(reader, writer));
    }

    protected Log getLog() {
        return env.getLog();
    }

    protected Pair<DataReader, DataWriter> createRW() throws IOException {
        final File testsDirectory = getEnvDirectory();
        if (testsDirectory.exists()) {
            IOUtil.deleteRecursively(testsDirectory);
        } else if (!testsDirectory.mkdir()) {
            throw new IOException("Failed to create directory for tests.");
        }
        return new Pair<DataReader, DataWriter>(
            new FileDataReader(testsDirectory, 16),
            new FileDataWriter(testsDirectory)
        );
    }

    protected void deleteRW() {
        final File testsDirectory = getEnvDirectory();
        IOUtil.deleteRecursively(testsDirectory);
        IOUtil.deleteFile(testsDirectory);
        envDirectory = null;
        reader = null;
        writer = null;
    }

    protected File getEnvDirectory() {
        if (envDirectory == null) {
            envDirectory = TestUtil.createTempDir();
        }
        return envDirectory;
    }

    protected void executeParallelTransaction(@NotNull final TransactionalExecutable runnable) {
        try {
            final Latch sync = Latch.create();
            sync.acquire();
            processor.queue(new Job() {
                @Override
                protected void execute() throws Throwable {
                    env.executeInTransaction(runnable);
                    sync.release();
                }
            });
            sync.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // ignore
        }
    }

    protected void runParallelRunnable(@NotNull final Runnable runnable) {
        processor.queue(new Job() {
            @Override
            protected void execute() throws Throwable {
                runnable.run();
            }
        });
    }

    protected Store openStoreAutoCommit(final String name, final StoreConfig config) {
        return env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull Transaction txn) {
                return env.openStore(name, config, txn);
            }
        });
    }

    protected void putAutoCommit(@NotNull final Store store,
                                 @NotNull final ByteIterable key,
                                 @NotNull final ByteIterable value) {
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                store.put(txn, key, value);
            }
        });
    }

    protected ByteIterable getAutoCommit(@NotNull final Store store,
                                         @NotNull final ByteIterable key) {
        return env.computeInReadonlyTransaction(new TransactionalComputable<ByteIterable>() {
            @Override
            public ByteIterable compute(@NotNull Transaction txn) {
                return store.get(txn, key);
            }
        });
    }

    protected boolean deleteAutoCommit(@NotNull final Store store,
                                       @NotNull final ByteIterable key) {
        return env.computeInTransaction(new TransactionalComputable<Boolean>() {
            @Override
            public Boolean compute(@NotNull Transaction txn) {
                return store.delete(txn, key);
            }
        });
    }

    protected long countAutoCommit(@NotNull final Store store) {
        return env.computeInTransaction(new TransactionalComputable<Long>() {
            @Override
            public Long compute(@NotNull Transaction txn) {
                return store.count(txn);
            }
        });
    }

    protected static void assertLoggableTypes(final Iterator<RandomAccessLoggable> it, final int... types) {
        assertLoggableTypes(Integer.MAX_VALUE, it, types);
    }

    protected static void assertLoggableTypes(final Log log, final int address, final int... types) {
        assertLoggableTypes(Integer.MAX_VALUE, log.getLoggableIterator(address), types);
    }

    protected static void assertLoggableTypes(final int max, final Iterator<RandomAccessLoggable> it, final int... types) {
        int i = 0;
        for (int type : types) {
            if (++i > max) {
                break;
            }
            Assert.assertTrue(it.hasNext());
            Assert.assertEquals(type, it.next().getType());
        }
        Assert.assertFalse(it.hasNext());
    }

    protected void assertNotNullStringValue(final Store store,
                                            final ByteIterable keyEntry,
                                            final String value) {
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                assertNotNullStringValue(txn, store, keyEntry, value);
            }
        });
    }

    protected void assertNotNullStringValue(final Transaction txn, final Store store,
                                            final ByteIterable keyEntry, final String value) {
        final ByteIterable valueEntry = store.get(txn, keyEntry);
        Assert.assertNotNull(valueEntry);
        Assert.assertEquals(value, StringBinding.entryToString(valueEntry));
    }

    protected void assertEmptyValue(final Store store, final ByteIterable keyEntry) {
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                assertEmptyValue(txn, store, keyEntry);
            }
        });
    }

    protected void assertEmptyValue(final Transaction txn, final Store store, final ByteIterable keyEntry) {
        final ByteIterable valueEntry = store.get(txn, keyEntry);
        Assert.assertNull(valueEntry);
    }

    protected void assertNotNullStringValues(final Store store,
                                             final String... values) {
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull Transaction txn) {
                try (Cursor cursor = store.openCursor(txn)) {
                    int i = 0;
                    while (cursor.getNext()) {
                        final ByteIterable valueEntry = cursor.getValue();
                        Assert.assertNotNull(valueEntry);
                        final String value = values[i++];
                        Assert.assertEquals(value, StringBinding.entryToString(valueEntry));
                    }
                }
            }
        });
    }

    protected void reopenEnvironment() {
        final EnvironmentConfig envConfig = env.getEnvironmentConfig();
        env.close();
        env = newEnvironmentInstance(LogConfig.create(reader, writer), envConfig);
    }

    protected void setLogFileSize(int kilobytes) {
        final EnvironmentConfig envConfig = env.getEnvironmentConfig();
        if (envConfig.getLogCachePageSize() > kilobytes * 1024) {
            envConfig.setLogCachePageSize(kilobytes * 1024);
        }
        envConfig.setLogFileSize(kilobytes);
        Log.invalidateSharedCache();
        reopenEnvironment();
    }

    protected void set1KbFileWithoutGC() {
        setLogFileSize(1);
        env.getEnvironmentConfig().setGcEnabled(false);
    }

    protected void set2KbFileWithoutGC() {
        setLogFileSize(2);
        env.getEnvironmentConfig().setGcEnabled(false);
    }

    private static void invalidateSharedCaches() {
        Log.invalidateSharedCache();
        try {
            SharedOpenFilesCache.invalidate();
            SharedMappedFilesCache.invalidate();
        } catch (IOException ignore) {
        }
    }
}
