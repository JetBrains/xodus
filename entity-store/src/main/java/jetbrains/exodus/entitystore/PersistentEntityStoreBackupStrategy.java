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

import jetbrains.exodus.backup.BackupStrategy;
import jetbrains.exodus.backup.VirtualFileDescriptor;
import jetbrains.exodus.log.LogUtil;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public class PersistentEntityStoreBackupStrategy extends BackupStrategy {

    private final PersistentStoreTransaction backupTxn;
    private final BackupStrategy environmentBackupStrategy;
    private final BackupStrategy blobVaultBackupStrategy;

    public PersistentEntityStoreBackupStrategy(@NotNull final PersistentEntityStoreImpl store) {
        backupTxn = store.beginReadonlyTransaction();
        final long logHighAddress = backupTxn.getEnvironmentTransaction().getHighAddress();
        environmentBackupStrategy = new BackupStrategyDecorator(store.getEnvironment().getBackupStrategy()) {
            @Override
            public long acceptFile(@NotNull final VirtualFileDescriptor file) {
                return Math.min(super.acceptFile(file), logHighAddress - LogUtil.getAddress(file.getName()));
            }
        };
        final BlobVault blobVault = store.getBlobVault().getSourceVault();
        if (!(blobVault instanceof FileSystemBlobVault)) {
            blobVaultBackupStrategy = blobVault.getBackupStrategy();
        } else {
            final FileSystemBlobVault fsBlobVault = (FileSystemBlobVault) blobVault;
            final long lastUsedHandle = store.getSequence(backupTxn, PersistentEntityStoreImpl.BLOB_HANDLES_SEQUENCE).loadValue(backupTxn);
            blobVaultBackupStrategy = new BackupStrategyDecorator(blobVault.getBackupStrategy()) {
                @Override
                public long acceptFile(@NotNull final VirtualFileDescriptor file) {
                    //noinspection AccessStaticViaInstance
                    if (!file.hasContent() || file.getName().equals(fsBlobVault.VERSION_FILE)) {
                        return super.acceptFile(file);
                    }
                    // TODO: improve this?
                    if ((file instanceof FileDescriptor) && fsBlobVault.getBlobHandleByFile(((FileDescriptor)file).getFile()) > lastUsedHandle) {
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
    public Iterable<VirtualFileDescriptor> getContents() {
        return new Iterable<VirtualFileDescriptor>() {
            @NotNull
            @Override
            public Iterator<VirtualFileDescriptor> iterator() {
                return new Iterator<VirtualFileDescriptor>() {

                    private Iterator<VirtualFileDescriptor> filesIterator = environmentBackupStrategy.getContents().iterator();
                    private boolean environmentListed = false;

                    @Override
                    public boolean hasNext() {
                        while (!filesIterator.hasNext()) {
                            if (environmentListed) {
                                return false;
                            }
                            environmentListed = true;
                            filesIterator = blobVaultBackupStrategy.getContents().iterator();
                        }
                        return true;
                    }

                    @Override
                    public VirtualFileDescriptor next() {
                        return filesIterator.next();
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @Override
    public void afterBackup() throws Exception {
        try {
            blobVaultBackupStrategy.afterBackup();
            environmentBackupStrategy.afterBackup();
        } finally {
            backupTxn.abort();
        }
    }

    @Override
    public long acceptFile(@NotNull final VirtualFileDescriptor file) {
        return LogUtil.isLogFileName(file.getName()) ? environmentBackupStrategy.acceptFile(file) : blobVaultBackupStrategy.acceptFile(file);
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
        public Iterable<VirtualFileDescriptor> getContents() {
            return decorated.getContents();
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
        public long acceptFile(@NotNull VirtualFileDescriptor file) {
            return decorated.acceptFile(file);
        }
    }
}
