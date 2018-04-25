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
package jetbrains.exodus.entitystore.replication

import jetbrains.exodus.backup.BackupStrategy
import jetbrains.exodus.core.dataStructures.hash.LongHashMap
import jetbrains.exodus.core.dataStructures.hash.LongSet
import jetbrains.exodus.crypto.EncryptedBlobVault
import jetbrains.exodus.entitystore.BlobVault
import jetbrains.exodus.entitystore.DiskBasedBlobVault
import jetbrains.exodus.entitystore.PersistentEntityStoreImpl
import jetbrains.exodus.env.Transaction
import java.io.File
import java.io.InputStream

class S3BlobVault(
        var delegate: DiskBasedBlobVault,
        val store: PersistentEntityStoreImpl,
        val replicator: S3Replicator
) : BlobVault(store.config), DiskBasedBlobVault {
    override fun getBackupStrategy(): BackupStrategy = BackupStrategy.EMPTY

    override fun getContent(blobHandle: Long, txn: Transaction): InputStream? {
        var result = delegate.getContent(blobHandle, txn)
        if (result == null) {
            store.getBlobFileLength(blobHandle, txn)?.let { length ->
                replicator.replicateBlob(blobHandle, length, delegate, replicator.sourceEncrypted, delegate is EncryptedBlobVault)?.let {
                    result = delegate.getContent(blobHandle, txn)
                }
            }
        }
        return result
    }

    // TODO: get the size from blobs table everywhere
    override fun getSize(blobHandle: Long, txn: Transaction): Long = store.getBlobFileLength(blobHandle, txn) ?: 0L

    override fun getBlobKey(blobHandle: Long): String {
        return delegate.getBlobKey(blobHandle)
    }

    override fun getBlobLocation(blobHandle: Long): File {
        return delegate.getBlobLocation(blobHandle)
    }

    override fun getBlobLocation(blobHandle: Long, readonly: Boolean): File {
        if (!readonly) {
            readOnly()
        }
        return delegate.getBlobLocation(blobHandle, true)
    }

    override fun size(): Long {
        return delegate.size()
    }

    override fun requiresTxn(): Boolean = false

    override fun nextHandle(txn: Transaction): Long {
        readOnly()
    }

    override fun flushBlobs(blobStreams: LongHashMap<InputStream>?, blobFiles: LongHashMap<File>?, deferredBlobsToDelete: LongSet?, txn: Transaction) {
        if ((blobFiles != null && blobFiles.isNotEmpty()) || (blobStreams != null && blobStreams.isNotEmpty())) {
            readOnly()
        }
    }

    override fun clear() = readOnly()

    override fun close() {
        delegate.close() // TODO?
    }

    private fun readOnly(): Nothing = throw UnsupportedOperationException("vault is read-only")
}
