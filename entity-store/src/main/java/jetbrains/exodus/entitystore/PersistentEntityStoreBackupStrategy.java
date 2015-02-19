/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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

import jetbrains.exodus.BackupStrategy;
import jetbrains.exodus.env.EnvironmentImpl;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogUtil;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class PersistentEntityStoreBackupStrategy extends BackupStrategy {

    private final Stack<PersistentStoreTransaction> nestedTxns;
    private final long logHighAddress;
    private final BackupStrategy environmentBackupStrategy;
    private final BackupStrategy blobVaultBackupStrategy;

    public PersistentEntityStoreBackupStrategy(@NotNull final PersistentEntityStoreImpl store) {
        nestedTxns = new Stack<PersistentStoreTransaction>();
        // TODO: rewrite this rather tricky optimistic evaluation of atomic pair: transaction and corresponding log high address
        final EnvironmentImpl env = (EnvironmentImpl) store.getEnvironment();
        final Log log = env.getLog();
        nestedTxns.push(store.beginReadonlyTransaction());
        while (true) {
            final long highAddress = log.getHighAddress();
            nestedTxns.push(store.beginReadonlyTransaction());
            if (log.getHighAddress() == highAddress) {
                logHighAddress = highAddress;
                break;
            }
        }
        environmentBackupStrategy = new BackupStrategyDecorator(env.getBackupStrategy()) {
            @Override
            public long acceptFile(@NotNull final File file) {
                return Math.min(super.acceptFile(file), logHighAddress - LogUtil.getAddress(file.getName()));
            }
        };
        final BlobVault blobVault = store.getBlobVault();
        if (!(blobVault instanceof FileSystemBlobVault)) {
            blobVaultBackupStrategy = blobVault.getBackupStrategy();
        } else {
            final PersistentStoreTransaction txn = nestedTxns.peek();
            final FileSystemBlobVault fsBlobVault = (FileSystemBlobVault) blobVault;
            final long lastUsedHandle = store.getSequence(txn, PersistentEntityStoreImpl.BLOB_HANDLES_SEQUENCE).loadValue(txn);
            blobVaultBackupStrategy = new BackupStrategyDecorator(blobVault.getBackupStrategy()) {
                @Override
                public long acceptFile(@NotNull final File file) {
                    //noinspection AccessStaticViaInstance
                    if (!file.isFile() || file.getName().equals(fsBlobVault.VERSION_FILE)) {
                        return super.acceptFile(file);
                    }
                    if (fsBlobVault.getBlobHandleByFile(file) > lastUsedHandle) {
                        return -1L;
                    }
                    return super.acceptFile(file);
                }
            };
        }
    }

    @Override
    public void beforeBackup() throws Exception {
        environmentBackupStrategy.beforeBackup();
        blobVaultBackupStrategy.beforeBackup();
    }

    @Override
    public Iterable<FileDescriptor> listFiles() {
        final List<FileDescriptor> allFiles = new ArrayList<FileDescriptor>();
        for (FileDescriptor blob : blobVaultBackupStrategy.listFiles()) {
            allFiles.add(blob);
        }
        for (FileDescriptor xdFile : environmentBackupStrategy.listFiles()) {
            allFiles.add(xdFile);
        }
        return allFiles;
    }

    @Override
    public void afterBackup() throws Exception {
        try {
            blobVaultBackupStrategy.afterBackup();
            environmentBackupStrategy.afterBackup();
        } finally {
            while (!nestedTxns.isEmpty()) {
                nestedTxns.pop().abort();
            }
        }
    }

    private static class BackupStrategyDecorator extends BackupStrategy {

        @NotNull
        private final BackupStrategy decorated;

        public BackupStrategyDecorator(@NotNull final BackupStrategy decorated) {
            this.decorated = decorated;
        }

        @Override
        public void beforeBackup() throws Exception {
            decorated.beforeBackup();
        }

        @Override
        public Iterable<FileDescriptor> listFiles() {
            return decorated.listFiles();
        }

        @Override
        public void afterBackup() throws Exception {
            decorated.afterBackup();
        }

        @Override
        public void onError(Throwable t) {
            decorated.onError(t);
        }

        @Override
        public long acceptFile(@NotNull File file) {
            return decorated.acceptFile(file);
        }
    }
}
