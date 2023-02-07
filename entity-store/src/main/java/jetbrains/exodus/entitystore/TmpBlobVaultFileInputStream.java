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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

/**
 * Stream which is backed by the temporary file created inside {@link DiskBasedBlobVault}.
 * @see DiskBasedBlobVault#copyToTemporaryStore(long, InputStream)
 * @see Entity#setBlob(String, InputStream)
 * @see PersistentEntityStoreConfig#DO_NOT_INVALIDATE_BLOB_STREAMS_ON_ROLLBACK
 */
final class TmpBlobVaultFileInputStream extends FileInputStream {
    private final Path path;
    private final long blobHandle;

    public TmpBlobVaultFileInputStream(final Path path, final long blobHandle) throws IOException {
        super(path.toFile());
        this.path = path;
        this.blobHandle = blobHandle;
    }

    public Path getPath() {
        return path;
    }

    public long getBlobHandle() {
        return blobHandle;
    }

}
