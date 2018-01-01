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

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;

public class FileSystemBlobVault extends FileSystemBlobVaultOld {

    private static final int EXPECTED_VERSION = 1;

    public FileSystemBlobVault(@NotNull final PersistentEntityStoreConfig config,
                               @NotNull final String parentDirectory,
                               @NotNull final String blobsDirectory,
                               @NotNull final String blobExtension,
                               @NotNull final BlobHandleGenerator blobHandleGenerator) throws IOException {
        super(config, parentDirectory, blobsDirectory, blobExtension, blobHandleGenerator, EXPECTED_VERSION);
    }

    @NotNull
    @Override
    protected File getBlobLocation(long blobHandle, boolean readonly) {
        if (blobHandle < 256) {
            return super.getBlobLocation(blobHandle, readonly);
        }
        final byte[] bytes = new byte[Long.SIZE / Byte.SIZE];
        int handleLen = 0;
        do {
            bytes[handleLen++] = (byte) (blobHandle & 0xff);
            blobHandle >>= 8;
        } while (blobHandle > 0);
        File dir = getVaultLocation();
        String file;
        while (true) {
            file = Integer.toHexString(bytes[--handleLen] & 0xff);
            if (handleLen == 0) {
                break;
            }
            dir = new File(dir, file);
        }

        if (!readonly) {
            //noinspection ResultOfMethodCallIgnored
            dir.mkdirs();
        }
        final File result = new File(dir, file + getBlobExtension());
        if (!readonly && result.exists()) {
            throw new EntityStoreException("Can't update existing blob file: " + result);
        }
        return result;
    }

    protected long getBlobHandleByFile(@NotNull final File file) {
        final String name = file.getName();
        final String blobExtension = getBlobExtension();
        final int blobExtensionStart = name.indexOf(blobExtension);
        if (name.endsWith(blobExtension) && (blobExtensionStart == 2 || blobExtensionStart == 1)) {
            try {
                long result = Integer.parseInt(name.substring(0, blobExtensionStart), 16);
                File f = file;
                int shift = 0;
                while (true) {
                    f = f.getParentFile();
                    if (f == null) {
                        break;
                    }
                    if (f.equals(getVaultLocation())) {
                        return result;
                    }
                    shift += Byte.SIZE;
                    result = result + (((long) Integer.parseInt(f.getName(), 16)) << shift);
                }
            } catch (NumberFormatException nfe) {
                throw new EntityStoreException("Not a file of filesystem blob vault: " + file, nfe);
            }
        }
        throw new EntityStoreException("Not a file of filesystem blob vault: " + file);
    }
}
