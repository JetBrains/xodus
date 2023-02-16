/*
 * Copyright 2010 - 2023 JetBrains s.r.o.
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

import org.jetbrains.annotations.NotNull;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Path;

/**
 * Stream which is backed by the temporary file created inside {@link DiskBasedBlobVault}.
 *
 * @see DiskBasedBlobVault#copyToTemporaryStore(long, InputStream, StoreTransaction)
 * @see Entity#setBlob(String, InputStream)
 * @see PersistentEntityStoreConfig#DO_NOT_INVALIDATE_BLOB_STREAMS_ON_ROLLBACK
 */
public class TmpBlobVaultBufferedInputStream  extends BufferedInputStream {
    private final Path path;
    private long blobHandle;
    private final PersistentStoreTransaction transaction;

    public TmpBlobVaultBufferedInputStream(@NotNull InputStream in, Path path, long blobHandle, PersistentStoreTransaction transaction) {
        super(in);
        this.path = path;
        this.blobHandle = blobHandle;
        this.transaction = transaction;
    }

    public Path getPath() {
        return path;
    }

    public long getBlobHandle() {
        return blobHandle;
    }

    public void setBlobHandle(long handle) {
        this.blobHandle = handle;
    }

    public PersistentStoreTransaction getTransaction() {
        return transaction;
    }
}
