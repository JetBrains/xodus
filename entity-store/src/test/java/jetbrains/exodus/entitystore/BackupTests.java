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
package jetbrains.exodus.entitystore;

import jetbrains.exodus.TestUtil;
import jetbrains.exodus.backup.BackupBean;
import jetbrains.exodus.backup.BackupStrategy;
import jetbrains.exodus.backup.Backupable;
import jetbrains.exodus.backup.VirtualFileDescriptor;
import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.core.execution.JobProcessor;
import jetbrains.exodus.core.execution.JobProcessorExceptionHandler;
import jetbrains.exodus.core.execution.ThreadJobProcessor;
import jetbrains.exodus.util.CompressBackupUtil;
import jetbrains.exodus.util.IOUtil;
import jetbrains.exodus.util.Random;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;

public class BackupTests extends EntityStoreTestBase {

    @Override
    protected boolean needsImplicitTxn() {
        return false;
    }

    public void testSingular() throws Exception {
        final PersistentEntityStoreImpl store = getEntityStore();
        store.getConfig().setMaxInPlaceBlobSize(0); // no in-place blobs
        final String randomDescription[] = {null};
        store.executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                final Entity issue = txn.newEntity("Issue");
                randomDescription[0] = Double.toString(Math.random());
                issue.setBlobString("description", randomDescription[0]);
            }
        });
        final File backupDir = TestUtil.createTempDir();
        try {
            final File backup = CompressBackupUtil.backup(store, backupDir, null, true);
            final File restoreDir = TestUtil.createTempDir();
            try {
                extractEntireZip(backup, restoreDir);
                final PersistentEntityStoreImpl newStore = PersistentEntityStores.newInstance(restoreDir);
                try {
                    newStore.executeInReadonlyTransaction(new StoreTransactionalExecutable() {
                        @Override
                        public void execute(@NotNull final StoreTransaction txn) {
                            assertEquals(1, txn.getAll("Issue").size());
                            final Entity issue = txn.getAll("Issue").getFirst();
                            assertNotNull(issue);
                            assertEquals(randomDescription[0], issue.getBlobString("description"));
                        }
                    });
                } finally {
                    newStore.close();
                }
            } finally {
                IOUtil.deleteRecursively(restoreDir);
            }
        } finally {
            IOUtil.deleteRecursively(backupDir);
        }
    }

    public void testStress() throws Exception {
        doStressTest(false);
    }

    public void testStressWithBackupBean() throws Exception {
        doStressTest(true);
    }

    public void testInterruptedIsDeleted() throws Exception {
        testSingular();
        final File backupDir = TestUtil.createTempDir();
        try {
            final BackupStrategy storeBackupStrategy = getEntityStore().getBackupStrategy();
            final File backup = CompressBackupUtil.backup(new Backupable() {
                @NotNull
                @Override
                public BackupStrategy getBackupStrategy() {
                    return new BackupStrategy() {
                        @Override
                        public void beforeBackup() throws Exception {
                            storeBackupStrategy.beforeBackup();
                        }

                        @Override
                        public Iterable<VirtualFileDescriptor> getContents() {
                            return storeBackupStrategy.getContents();
                        }

                        @Override
                        public void afterBackup() throws Exception {
                            storeBackupStrategy.afterBackup();
                        }

                        @Override
                        public boolean isInterrupted() {
                            return true;
                        }

                        @Override
                        public long acceptFile(@NotNull VirtualFileDescriptor file) {
                            return storeBackupStrategy.acceptFile(file);
                        }
                    };
                }
            }, backupDir, null, true);
            assertFalse(backup.exists());
        } finally {
            IOUtil.deleteRecursively(backupDir);
        }
    }

    public void doStressTest(final boolean useBackupBean) throws Exception {
        final PersistentEntityStoreImpl store = getEntityStore();
        store.getConfig().setMaxInPlaceBlobSize(0); // no in-place blobs
        final int issueCount = 1000;
        store.executeInTransaction(new StoreTransactionalExecutable() {
            @Override
            public void execute(@NotNull StoreTransaction txn) {
                for (int i = 0; i < issueCount; ++i) {
                    final Entity issue = txn.newEntity("Issue");
                    issue.setBlobString("description", Double.toString(Math.random()));
                }
            }
        });
        final Random rnd = new Random();
        final boolean[] finish = {false};
        final int[] backgroundChanges = {0};
        final int threadCount = 4;
        final ThreadJobProcessor[] threads = new ThreadJobProcessor[threadCount];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new ThreadJobProcessor("BackupTest Job Processor " + i);
            threads[i].start();
            threads[i].setExceptionHandler(new JobProcessorExceptionHandler() {
                @Override
                public void handle(JobProcessor processor, Job job, Throwable t) {
                    System.out.println(t.toString());
                }
            });
            threads[i].queue(new Job() {
                @Override
                protected void execute() throws Throwable {
                    while (!finish[0]) {
                        store.executeInTransaction(new StoreTransactionalExecutable() {
                            @Override
                            public void execute(@NotNull final StoreTransaction txn) {
                                final Entity issue = txn.getAll("Issue").skip(rnd.nextInt(issueCount - 1)).getFirst();
                                assertNotNull(issue);
                                issue.setBlobString("description", Double.toString(Math.random()));
                                System.out.print("\r" + (++backgroundChanges[0]));
                            }
                        });
                    }
                }
            });
        }
        Thread.sleep(1000);
        final File backupDir = TestUtil.createTempDir();
        try {
            final File backup = CompressBackupUtil.backup(useBackupBean ? new BackupBean(store) : store, backupDir, null, true);
            finish[0] = true;
            final File restoreDir = TestUtil.createTempDir();
            try {
                extractEntireZip(backup, restoreDir);
                final PersistentEntityStoreImpl newStore = PersistentEntityStores.newInstance(restoreDir);
                try {
                    final long[] lastUsedBlobHandle = {-1L};
                    newStore.executeInReadonlyTransaction(new StoreTransactionalExecutable() {
                        @Override
                        public void execute(@NotNull final StoreTransaction t) {
                            final PersistentStoreTransaction txn = (PersistentStoreTransaction) t;
                            assertEquals(issueCount, txn.getAll("Issue").size());
                            lastUsedBlobHandle[0] = newStore.getSequence(txn, PersistentEntityStoreImpl.BLOB_HANDLES_SEQUENCE).loadValue(txn);
                            for (final Entity issue : txn.getAll("Issue")) {
                                final String description = issue.getBlobString("description");
                                assertNotNull(description);
                                assertFalse(description.isEmpty());
                            }
                        }
                    });
                    final FileSystemBlobVault blobVault = (FileSystemBlobVault) newStore.getBlobVault().getSourceVault();
                    for (final VirtualFileDescriptor fd : blobVault.getBackupStrategy().getContents()) {
                        final File file = ((BackupStrategy.FileDescriptor) fd).getFile();
                        if (file.isFile() && !file.getName().equals(FileSystemBlobVaultOld.VERSION_FILE)) {
                            assertTrue("" + blobVault.getBlobHandleByFile(file) + " > " + lastUsedBlobHandle[0],
                                    blobVault.getBlobHandleByFile(file) <= lastUsedBlobHandle[0]);
                        }
                    }
                } finally {
                    newStore.close();
                }
            } finally {
                IOUtil.deleteRecursively(restoreDir);
            }
        } finally {
            IOUtil.deleteRecursively(backupDir);
        }
        for (final ThreadJobProcessor thread : threads) {
            thread.finish();
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void extractEntireZip(@NotNull final File zip, @NotNull final File restoreDir) throws IOException {
        try (ZipFile zipFile = new ZipFile(zip)) {
            final Enumeration<ZipArchiveEntry> zipEntries = zipFile.getEntries();
            while (zipEntries.hasMoreElements()) {
                final ZipArchiveEntry zipEntry = zipEntries.nextElement();
                final File entryFile = new File(restoreDir, zipEntry.getName());
                if (zipEntry.isDirectory()) {
                    entryFile.mkdirs();
                } else {
                    entryFile.getParentFile().mkdirs();
                    try (FileOutputStream target = new FileOutputStream(entryFile)) {
                        try (InputStream in = zipFile.getInputStream(zipEntry)) {
                            IOUtil.copyStreams(in, target, IOUtil.BUFFER_ALLOCATOR);
                        }
                    }
                }
            }
        }
    }

}
