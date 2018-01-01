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
import jetbrains.exodus.bindings.IntegerBinding;
import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.core.dataStructures.hash.*;
import jetbrains.exodus.entitystore.tables.BlobsTable;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.util.IOUtil;
import jetbrains.exodus.vfs.ClusteringStrategy;
import jetbrains.exodus.vfs.VfsConfig;
import jetbrains.exodus.vfs.VfsException;
import jetbrains.exodus.vfs.VirtualFileSystem;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.util.Map;

public class VFSBlobVault extends BlobVault {

    private static final VfsConfig BLOB_VAULT_VFS_CONFIG;

    static {
        BLOB_VAULT_VFS_CONFIG = new VfsConfig();
        BLOB_VAULT_VFS_CONFIG.setClusteringStrategy(ClusteringStrategy.EXPONENTIAL);
    }

    @NotNull
    private final VirtualFileSystem fs;

    public VFSBlobVault(@NotNull final PersistentEntityStoreConfig config, @NotNull final Environment env) {
        this(config, new VirtualFileSystem(env, BLOB_VAULT_VFS_CONFIG));
    }

    public VFSBlobVault(@NotNull final PersistentEntityStoreConfig config, @NotNull final VirtualFileSystem fs) {
        super(config);
        this.fs = fs;
    }

    @Override
    public long nextHandle(@NotNull final Transaction txn) {
        return fs.createFile(txn, "blob.%d").getDescriptor();
    }

    public void setContent(long blobHandle, @NotNull InputStream content, @NotNull final Transaction txn) throws Exception {
        OutputStream blobOutput = null;
        try {
            blobOutput = fs.writeFile(txn, blobHandle);
            IOUtil.copyStreams(content, blobOutput, bufferAllocator);
        } finally {
            if (blobOutput != null) {
                blobOutput.close();
            }
        }
    }

    public void setContent(long blobHandle, @NotNull File file, @NotNull final Transaction txn) throws Exception {
        OutputStream blobOutput = null;
        try {
            blobOutput = fs.writeFile(txn, blobHandle);
            IOUtil.copyStreams(new FileInputStream(file), blobOutput, bufferAllocator);
        } finally {
            if (blobOutput != null) {
                blobOutput.close();
            }
        }
    }

    @Override
    public InputStream getContent(long blobHandle, @NotNull final Transaction txn) {
        return fs.readFile(txn, blobHandle);
    }

    @Override
    public long getSize(long blobHandle, @NotNull Transaction txn) {
        return fs.getFileLength(txn, blobHandle);
    }

    public boolean delete(long blobHandle, @NotNull final Transaction txn) {
        return fs.deleteFile(txn, "blob." + blobHandle) != null;
    }

    @Override
    public boolean requiresTxn() {
        return true;
    }

    @Override
    public void flushBlobs(@Nullable final LongHashMap<InputStream> blobStreams,
                           @Nullable final LongHashMap<File> blobFiles,
                           @Nullable final LongSet deferredBlobsToDelete,
                           @NotNull final Transaction txn) throws Exception {
        if (blobStreams != null) {
            blobStreams.forEachEntry(new ObjectProcedureThrows<Map.Entry<Long, InputStream>, Exception>() {
                @Override
                public boolean execute(final Map.Entry<Long, InputStream> object) throws Exception {
                    final InputStream stream = object.getValue();
                    stream.reset();
                    setContent(object.getKey(), stream, txn);
                    return true;
                }
            });
        }
        // if there were blob files then move them
        if (blobFiles != null) {
            blobFiles.forEachEntry(new ObjectProcedureThrows<Map.Entry<Long, File>, Exception>() {
                @Override
                public boolean execute(final Map.Entry<Long, File> object) throws Exception {
                    setContent(object.getKey(), object.getValue(), txn);
                    return true;
                }
            });
        }
        // if there are deferred blobs to delete then defer their deletion
        if (deferredBlobsToDelete != null) {
            try {
                final LongIterator it = deferredBlobsToDelete.iterator();
                while (it.hasNext()) {
                    delete(it.nextLong(), txn);
                }
            } finally {
                txn.abort();
            }
        }
    }

    @Override
    public long size() {
        return 0; // zero 'cause we just rely on data environment size
    }

    @Override
    public void clear() {
        // do nothing since we rely on Environment.clear()
    }

    @Override
    public void close() {
        fs.shutdown();
    }

    @NotNull
    @Override
    public BackupStrategy getBackupStrategy() {
        return BackupStrategy.EMPTY;
    }

    public void refactorFromFS(@NotNull final PersistentEntityStoreImpl store) throws IOException {
        final BlobVault sourceVault = new FileSystemBlobVaultOld(store.getConfig(), store.getLocation(),
            "blobs", ".blob", BlobHandleGenerator.IMMUTABLE);

        final LongSet allBlobs = store.computeInReadonlyTransaction(new StoreTransactionalComputable<LongSet>() {
            @Override
            public LongSet compute(@NotNull final StoreTransaction txn) {
                return loadAllBlobs(store, (PersistentStoreTransaction) txn);
            }
        });
        final Environment env = fs.getEnvironment();
        final Transaction txn = env.beginTransaction();
        try {
            int i = 0;
            for (final long blobId : allBlobs) {
                if (i++ % 100 == 0) {
                    txn.flush();
                }
                final InputStream content = sourceVault.getContent(blobId, txn);
                if (content != null) {
                    importBlob(txn, blobId, content);
                }
            }
            txn.flush();
        } catch (final IOException ioe) {
            throw new EntityStoreException(ioe);
        } finally {
            txn.abort();
        }
    }

    private void importBlob(final Transaction txn, final long blobHandle, @NotNull InputStream content) throws IOException {
        if (txn == null) {
            throw new VfsException("Can't import blob without a transaction");
        }
        fs.createFile(txn, blobHandle, "blob." + blobHandle);
        OutputStream blobOutput = null;
        try {
            blobOutput = fs.writeFile(txn, blobHandle);
            IOUtil.copyStreams(content, blobOutput, bufferAllocator);
        } finally {
            if (blobOutput != null) {
                blobOutput.close();
            }
        }
    }

    @NotNull
    private static LongSet loadAllBlobs(@NotNull final PersistentEntityStoreImpl store, @NotNull final PersistentStoreTransaction txn) {
        final LongSet result = new PackedLongHashSet();
        final Transaction envTxn = txn.getEnvironmentTransaction();
        try (Cursor entityTypesCursor = store.getEntityTypesTable().getSecondIndexCursor(envTxn)) {
            while (entityTypesCursor.getNext()) {
                final int entityTypeId = IntegerBinding.compressedEntryToInt(entityTypesCursor.getKey());
                final BlobsTable blobs = store.getBlobsTable(txn, entityTypeId);
                final Store primary = blobs.getPrimaryIndex();
                try (Cursor blobsCursor = primary.openCursor(envTxn)) {
                    while (blobsCursor.getNext()) {
                        final long blobId = LongBinding.compressedEntryToLong(blobsCursor.getValue());
                        result.add(blobId);
                    }
                }
            }
            return result;
        }
    }
}
