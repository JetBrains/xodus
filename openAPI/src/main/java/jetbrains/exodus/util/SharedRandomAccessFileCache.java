/**
 * Copyright 2010 - 2015 JetBrains s.r.o.
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
package jetbrains.exodus.util;

import jetbrains.exodus.core.dataStructures.ObjectCache;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

public class SharedRandomAccessFileCache {

    private final ObjectCache<File, SharedRandomAccessFile> cache;

    public SharedRandomAccessFileCache(final int keepFilesOpen) {
        cache = new ObjectCache<File, SharedRandomAccessFile>(keepFilesOpen);
    }

    public SharedRandomAccessFile getFile(@NotNull final File file) throws IOException {
        SharedRandomAccessFile result;
        lock();
        try {
            result = cache.tryKey(file);
            if (result != null && result.employ() > 1) {
                result.close();
                result = null;
            }
        } finally {
            unlock();
        }
        if (result == null) {
            result = createNewFile(file);
            SharedRandomAccessFile obsolete = null;
            lock();
            try {
                if (cache.getObject(file) == null) {
                    result.employ();
                    obsolete = cache.cacheObject(file, result);
                }
            } finally {
                unlock();
                if (obsolete != null) {
                    obsolete.close();
                }
            }
        }
        return result;
    }

    public void close() throws IOException {
        final Iterator<SharedRandomAccessFile> values = cache.values();
        while (values.hasNext()) {
            values.next().close();
        }
        cache.clear();
    }

    public void lock() {
    }

    public void unlock() {
    }

    /* By default, create read-only files
     */
    protected SharedRandomAccessFile createNewFile(@NotNull final File file) throws FileNotFoundException {
        return new SharedRandomAccessFile(file, "r");
    }
}
