/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import jetbrains.exodus.log.StartupMetadata;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Iterator;

public class PersistentEntityStoreBackupStrategy extends BackupStrategy {
    private final BackupStrategy environmentBackupStrategy;
    private final BackupStrategy blobVaultBackupStrategy;
    private final PersistentEntityStoreImpl store;
    private long lastUsedHandle;

    public PersistentEntityStoreBackupStrategy(@NotNull final PersistentEntityStoreImpl store) {
        this.store = store;
        environmentBackupStrategy = new BackupStrategyDecorator(store.getEnvironment().getBackupStrategy());

        final BlobVault blobVault = store.getBlobVault().getSourceVault();
        if (!(blobVault instanceof FileSystemBlobVault)) {
            blobVaultBackupStrategy = blobVault.getBackupStrategy();
        } else {
            final FileSystemBlobVault fsBlobVault = (FileSystemBlobVault) blobVault;
            blobVaultBackupStrategy = new BackupStrategyDecorator(blobVault.getBackupStrategy()) {
                @Override
                public long acceptFile(@NotNull final VirtualFileDescriptor file) {
                    //noinspection AccessStaticViaInstance
                    if (!file.hasContent() || file.getName().equals(fsBlobVault.VERSION_FILE)) {
                        return super.acceptFile(file);
                    }
                    final File f = file.getFile();
                    if (f != null && fsBlobVault.getBlobHandleByFile(f) > lastUsedHandle) {
                        return -1L;
                    }
                    return super.acceptFile(file);
                }
            };
        }
    }

    @Override
    public void beforeBackup() throws Exception {
        //Environment backup strategy suspends GC
        //but if GC is running we will need to wait inside of exclusive
        //transaction to avoid such behaviour
        store.getEnvironment().suspendGC();

        //We need to get a consistent data both from blob vault and
        //environment so we need to get them inside of exclusive transaction
        //to avoid race conditions caused in entity store transaction commit
        final PersistentStoreTransaction txn = store.beginExclusiveTransaction();
        try {
            environmentBackupStrategy.beforeBackup();
            blobVaultBackupStrategy.beforeBackup();

            //fetch last blob handle to filter newly added blobs during backup
            final BlobVault blobVault = store.getBlobVault().getSourceVault();
            if (blobVault instanceof FileSystemBlobVault) {
                lastUsedHandle = store.getSequence(txn,
                        PersistentEntityStoreImpl.BLOB_HANDLES_SEQUENCE).loadValue(txn);
                store.ensureBlobsConsistency(txn);
            }

        } finally {
            txn.abort();
        }
    }

    @Override
    public Iterable<VirtualFileDescriptor> getContents() {
        return () -> new Iterator<>() {

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

    @Override
    public void afterBackup() throws Exception {
        store.getEnvironment().resumeGC();

        blobVaultBackupStrategy.afterBackup();
        environmentBackupStrategy.afterBackup();
    }

    @Override
    public long acceptFile(@NotNull final VirtualFileDescriptor file) {
        return LogUtil.isLogFileName(file.getName()) || StartupMetadata.isStartupFileName(file.getName()) ?
                environmentBackupStrategy.acceptFile(file) :
                blobVaultBackupStrategy.acceptFile(file);
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
