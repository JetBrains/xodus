/**
 * Copyright 2010 - 2021 JetBrains s.r.o.
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
package jetbrains.exodus.entitystore

import jetbrains.exodus.core.dataStructures.hash.LongHashMap
import jetbrains.exodus.core.dataStructures.hash.LongSet
import jetbrains.exodus.env.Transaction
import java.io.File
import java.io.InputStream

/**
 * A blob vault that stores no blobs. Is intended to be used with in-memory databases for which all blobs are stored
 * as in-place blobs (see [PersistentEntityStoreConfig.MAX_IN_PLACE_BLOB_SIZE]).
 */
class DummyBlobVault(config: PersistentEntityStoreConfig) : BlobVault(config) {

    override fun delete(blobHandle: Long): Boolean = throw NotImplementedError()

    override fun clear() {}

    override fun getBackupStrategy() = throw NotImplementedError()

    override fun getContent(blobHandle: Long, txn: Transaction) = throw NotImplementedError()

    override fun getSize(blobHandle: Long, txn: Transaction) = throw NotImplementedError()

    override fun requiresTxn() = false

    override fun flushBlobs(blobStreams: LongHashMap<InputStream>?,
                            blobFiles: LongHashMap<File>?,
                            deferredBlobsToDelete:
                            LongSet?, txn: Transaction) = throw NotImplementedError()

    override fun size() = 0L

    override fun nextHandle(txn: Transaction) = throw NotImplementedError()

    override fun close() {}
    override fun getBlob(blobHandle: Long): BlobVaultItem = throw NotImplementedError()
}