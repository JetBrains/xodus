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
import jetbrains.exodus.core.dataStructures.hash.LinkedHashMap;
import jetbrains.exodus.system.OperatingSystem;
import jetbrains.exodus.util.SharedRandomAccessFile;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class SharedMappedFilesCache {

    private static final Object syncObject = new Object();
    @Nullable
    private static volatile SharedMappedFilesCache theCache = null;

    private final long freePhysicalMemoryThreshold;
    private final ConcurrentLinkedQueue<SharedMappedByteBuffer> obsoleteQueue;
    private final LinkedHashMap<File, SharedMappedByteBuffer> cache;

    private SharedMappedFilesCache(final long freePhysicalMemoryThreshold) {
        this.freePhysicalMemoryThreshold = freePhysicalMemoryThreshold;
        obsoleteQueue = new ConcurrentLinkedQueue<>();
        cache = new LinkedHashMap<File, SharedMappedByteBuffer>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<File, SharedMappedByteBuffer> eldest) {
                return isOSOverloaded();
            }

            @Override
            public SharedMappedByteBuffer remove(Object key) {
                final SharedMappedByteBuffer obsolete = super.remove(key);
                if (obsolete != null) {
                    obsoleteQueue.offer(obsolete);
                }
                return obsolete;
            }
        };
    }

    static void createInstance(final long freePhysicalMemoryThreshold) {
        if (theCache == null) {
            synchronized (syncObject) {
                if (theCache == null) {
                    theCache = new SharedMappedFilesCache(freePhysicalMemoryThreshold);
                }
            }
        }
    }

    @NotNull
    static SharedMappedFilesCache getInstance() {
        SharedMappedFilesCache result = theCache;
        if (result == null) {
            throw new ExodusException("SharedMappedFilesCache instance should be created explicitly");
        }
        return result;
    }

    /**
     * For tests only!!!
     */
    public static void invalidate() throws IOException {
        final SharedMappedFilesCache oldCache;
        synchronized (syncObject) {
            oldCache = SharedMappedFilesCache.theCache;
            SharedMappedFilesCache.theCache = null;
        }
        if (oldCache != null) {
            for (final SharedMappedByteBuffer buffer : oldCache.cache.values()) {
                buffer.close();
            }
        }
    }

    @NotNull
    SharedMappedByteBuffer getFileBuffer(@NotNull final SharedRandomAccessFile file) throws IOException {
        try {
            SharedMappedByteBuffer result;
            final File key = file.getFile();
            synchronized (cache) {
                result = cache.get(key);
                if (result != null) {
                    // we do employ() in the critical section intentionally, in order to avoid possible
                    // (though rather theoretical) race with closing this buffer when it becomes obsolete
                    result.employ();
                    return result;
                }
            }
            result = new SharedMappedByteBuffer(file);
            result.employ();
            final SharedMappedByteBuffer obsolete;
            synchronized (cache) {
                obsolete = cache.put(key, result);
            }
            if (obsolete != null) {
                obsoleteQueue.offer(obsolete);
            }
            return result;
        } finally {
            freeObsoleteBuffers();
        }
    }

    void removeFileBuffer(@NotNull final File file) {
        try {
            final SharedMappedByteBuffer obsolete;
            synchronized (cache) {
                obsolete = cache.remove(file);
            }
            if (obsolete != null) {
                obsoleteQueue.offer(obsolete);
            }
        } finally {
            freeObsoleteBuffers();
        }
    }

    void removeDirectory(@NotNull final File dir) throws IOException {
        try {
            final List<SharedMappedByteBuffer> result = new ArrayList<>();
            final List<File> obsoleteFiles = new ArrayList<>();
            synchronized (cache) {
                for (File file : cache.keySet()) {
                    if (file.getParentFile().equals(dir)) {
                        obsoleteFiles.add(file);
                        result.add(cache.get(file));
                    }
                }
                for (final File file : obsoleteFiles) {
                    cache.remove(file);
                }
            }
            for (final SharedMappedByteBuffer obsolete : result) {
                obsoleteQueue.offer(obsolete);
            }
        } finally {
            freeObsoleteBuffers();
        }
    }

    private void freeObsoleteBuffers() {
        SharedMappedByteBuffer obsolete;
        while (true) {
            while ((obsolete = obsoleteQueue.poll()) != null) {
                obsolete.close();
            }
            if (!isOSOverloaded()) {
                break;
            }
            synchronized (cache) {
                if (cache.size() == 0) {
                    break;
                }
                cache.removeEldest();
            }
        }
    }

    private boolean isOSOverloaded() {
        return OperatingSystem.INSTANCE.getFreePhysicalMemorySize() < freePhysicalMemoryThreshold;
    }
}
