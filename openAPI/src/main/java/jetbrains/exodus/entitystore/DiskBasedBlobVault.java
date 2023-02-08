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

import jetbrains.exodus.env.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

public interface DiskBasedBlobVault {

    File getBlobLocation(long blobHandle);

    File getBlobLocation(long blobHandle, boolean readonly);

    String getBlobKey(long blobHandle);

    boolean delete(long blobHandle);

    @Nullable InputStream getContent(long blobHandle, @NotNull Transaction txn,
                                     @Nullable Long expectedContentLength);

    long size();

    void close();

    @NotNull Path copyToTemporaryStore(long handle, final @NotNull InputStream stream) throws IOException;

    @NotNull InputStream openTmpStream(long handle,@NotNull Path path) throws IOException;
}
