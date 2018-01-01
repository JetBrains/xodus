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
package jetbrains.exodus.io;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.core.dataStructures.ObjectCache;
import jetbrains.exodus.util.SharedRandomAccessFile;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static jetbrains.exodus.core.dataStructures.ObjectCacheBase.CriticalSection;

public final class SharedOpenFilesCache {

    private static final Object syncObject = new Object();
    private static int cacheSize = 0;
    private static volatile SharedOpenFilesCache theCache = null;

    private final ObjectCache<File, SharedRandomAccessFile> cache;

    private SharedOpenFilesCache(final int openFiles) {
        cache = new ObjectCache<>(openFiles);
    }

    static void setSize(final int cacheSize) {
        if (cacheSize <= 0) {
            throw new IllegalArgumentException("Cache size must be a positive integer value");
        }
        SharedOpenFilesCache.cacheSize = cacheSize;
    }

    static SharedOpenFilesCache getInstance() {
        if (cacheSize == 0) {
            throw new ExodusException("Size of SharedOpenFilesCache is not set");
        }
        SharedOpenFilesCache result = theCache;
        if (result == null) {
            synchronized (syncObject) {
                result = theCache;
                if (result == null) {
                    result = theCache = new SharedOpenFilesCache(cacheSize);
                }
            }
        }
        return result;
    }

    /**
     * For tests only!!!
     */
    public static void invalidate() throws IOException {
        final SharedOpenFilesCache obsolete;
        synchronized (syncObject) {
            obsolete = theCache;
            theCache = null;
        }
        if (obsolete != null) {
            obsolete.clear();
        }
    }

    @NotNull
    SharedRandomAccessFile getCachedFile(@NotNull final File file) throws IOException {
        SharedRandomAccessFile result;
        try (CriticalSection ignored = cache.newCriticalSection()) {
            result = cache.tryKey(file);
            if (result != null && result.employ() > 1) {
                result.close();
                result = null;
            }
        }
        if (result == null) {
            result = new SharedRandomAccessFile(file, "r");
            SharedRandomAccessFile obsolete = null;
            try (CriticalSection ignored = cache.newCriticalSection()) {
                if (cache.getObject(file) == null) {
                    result.employ();
                    obsolete = cache.cacheObject(file, result);
                }
            }
            if (obsolete != null) {
                obsolete.close();
            }
        }
        return result;
    }

    void removeFile(@NotNull final File file) throws IOException {
        final SharedRandomAccessFile result;
        try (CriticalSection ignored = cache.newCriticalSection()) {
            result = cache.remove(file);
        }
        if (result != null) {
            result.close();
        }
    }

    void removeDirectory(@NotNull final File dir) throws IOException {
        final List<SharedRandomAccessFile> result = new ArrayList<>();
        final List<File> obsoleteFiles = new ArrayList<>();
        try (CriticalSection ignored = cache.newCriticalSection()) {
            final Iterator<File> keys = cache.keys();
            while (keys.hasNext()) {
                final File file = keys.next();
                if (file.getParentFile().equals(dir)) {
                    obsoleteFiles.add(file);
                    result.add(cache.getObject(file));
                }
            }
            for (final File file : obsoleteFiles) {
                cache.remove(file);
            }
        }
        for (final SharedRandomAccessFile obsolete : result) {
            obsolete.close();
        }
    }

    private void clear() throws IOException {
        final List<SharedRandomAccessFile> openFiles = new ArrayList<>();
        try (CriticalSection ignored = cache.newCriticalSection()) {
            final Iterator<SharedRandomAccessFile> it = cache.values();
            while (it.hasNext()) {
                openFiles.add(it.next());
            }
            cache.clear();
        }
        for (final SharedRandomAccessFile file : openFiles) {
            file.close();
        }
    }
}
