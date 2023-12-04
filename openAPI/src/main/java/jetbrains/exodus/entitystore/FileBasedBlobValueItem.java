/*
 * Copyright ${inceptionYear} - ${year} ${owner}
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

import java.io.File;

public class FileBasedBlobValueItem implements BlobVaultItem {

    @NotNull
    private final File file;
    private final long handle;

    public FileBasedBlobValueItem(@NotNull File file, long handle) {
        this.handle = handle;
        this.file = file;
    }

    @Override
    public long getHandle() {
        return handle;
    }

    @Override
    public @NotNull String getLocation() {
        return file.getAbsolutePath();
    }

    @Override
    public boolean exists() {
        return file.exists();
    }

    @Override
    public String toString() {
        return getLocation();
    }
}
