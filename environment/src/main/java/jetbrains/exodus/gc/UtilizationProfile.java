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
package jetbrains.exodus.gc;

import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.core.dataStructures.Priority;
import jetbrains.exodus.core.dataStructures.hash.LongHashMap;
import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.env.*;
import jetbrains.exodus.log.*;
import jetbrains.exodus.tree.LongIterator;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public final class UtilizationProfile {

    @NotNull
    private final EnvironmentImpl env;
    @NotNull
    private final GarbageCollector gc;
    @NotNull
    private final Log log;
    private final long fileSize; // in bytes
    @NotNull
    private final TreeMap<Long, FileUtilization> filesUtilization; // file address -> FileUtilization object
    private long totalBytes;
    private long totalFreeBytes;

    UtilizationProfile(@NotNull final EnvironmentImpl env, @NotNull final GarbageCollector gc) {
        this.env = env;
        this.gc = gc;
        log = env.getLog();
        fileSize = log.getFileSize() * 1024L;
        filesUtilization = new TreeMap<>();
        log.addNewFileListener(new NewFileListener() {
            @Override
            @SuppressWarnings({"ConstantConditions"})
            public void fileCreated(long fileAddress) {
                synchronized (filesUtilization) {
                    filesUtilization.put(fileAddress, new FileUtilization());
                }
                estimateTotalBytes();
            }
        });
    }

    /**
     * Loads utilization profile.
     */
    public void load() {
        if (env.getEnvironmentConfig().getGcUtilizationFromScratch()) {
            computeUtilizationFromScratch();
        } else {
            env.executeInReadonlyTransaction(new TransactionalExecutable() {
                @Override
                public void execute(@NotNull final Transaction txn) {
                    if (!env.storeExists(GarbageCollector.UTILIZATION_PROFILE_STORE_NAME, txn)) {
                        computeUtilizationFromScratch();
                    } else {
                        final Map<Long, FileUtilization> filesUtilization = new TreeMap<>();
                        final StoreImpl store = env.openStore(GarbageCollector.UTILIZATION_PROFILE_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, txn);
                        try (Cursor cursor = store.openCursor(txn)) {
                            while (cursor.getNext()) {
                                final long fileAddress = LongBinding.compressedEntryToLong(cursor.getKey());
                                final long freeBytes = CompressedUnsignedLongByteIterable.getLong(cursor.getValue());
                                filesUtilization.put(fileAddress, new FileUtilization(freeBytes));
                            }
                        }
                        synchronized (UtilizationProfile.this.filesUtilization) {
                            UtilizationProfile.this.filesUtilization.clear();
                            UtilizationProfile.this.filesUtilization.putAll(filesUtilization);
                        }
                    }
                }
            });
        }
        estimateTotalBytes();
        if (gc.isTooMuchFreeSpace()) {
            gc.wake();
        }
    }

    /**
     * Reloads utilization profile.
     */
    public void computeUtilizationFromScratch() {
        gc.getCleaner().getJobProcessor().queue(new Job() {
            @Override
            protected void execute() throws Throwable {
                final TreeMap<Long, Long> usedSpace = new TreeMap<>();
                env.executeInReadonlyTransaction(new TransactionalExecutable() {
                    @Override
                    public void execute(@NotNull Transaction txn) {
                        for (final String storeName : env.getAllStoreNames(txn)) {
                            final StoreImpl store = env.openStore(storeName, StoreConfig.USE_EXISTING, txn);
                            final LongIterator it = ((TransactionBase) txn).getTree(store).addressIterator();
                            while (it.hasNext()) {
                                final long address = it.next();
                                final RandomAccessLoggable loggable = log.read(address);
                                final Long fileAddress = log.getFileAddress(address);
                                Long usedBytes = usedSpace.get(fileAddress);
                                if (usedBytes == null) {
                                    usedBytes = 0L;
                                }
                                usedBytes += loggable.length();
                                usedSpace.put(fileAddress, usedBytes);
                            }
                        }
                    }
                });
                synchronized (filesUtilization) {
                    filesUtilization.clear();
                    for (final Map.Entry<Long, Long> entry : usedSpace.entrySet()) {
                        filesUtilization.put(entry.getKey(), new FileUtilization(fileSize - entry.getValue()));
                    }
                }
            }
        }, Priority.highest);
    }

    /**
     * Saves utilization profile in internal store.
     */
    void save() {
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                final StoreImpl store = env.openStore(GarbageCollector.UTILIZATION_PROFILE_STORE_NAME,
                        StoreConfig.WITHOUT_DUPLICATES, txn);
                // clear entries for already deleted files
                try (Cursor cursor = store.openCursor(txn)) {
                    while (cursor.getNext()) {
                        final long fileAddress = LongBinding.compressedEntryToLong(cursor.getKey());
                        final boolean fileIsDeleted;
                        synchronized (filesUtilization) {
                            fileIsDeleted = !filesUtilization.containsKey(fileAddress);
                        }
                        if (fileIsDeleted) {
                            cursor.deleteCurrent();
                        }
                    }
                }
                // save profile of up-to-date files
                final List<Map.Entry<Long, FileUtilization>> filesUtilization;
                synchronized (UtilizationProfile.this.filesUtilization) {
                    filesUtilization = new ArrayList<>(UtilizationProfile.this.filesUtilization.entrySet());
                }
                for (final Map.Entry<Long, FileUtilization> entry : filesUtilization) {
                    store.put(txn,
                            LongBinding.longToCompressedEntry(entry.getKey()),
                            CompressedUnsignedLongByteIterable.getIterable(entry.getValue().getFreeBytes()));
                }
            }
        });
    }

    int totalFreeSpacePercent() {
        final long totalBytes = this.totalBytes;
        return (int) (totalBytes == 0 ? 0 : ((totalFreeBytes * 100L) / totalBytes));
    }

    /**
     * @return for given files, map from file address to number of free bytes in it.
     */
    LongHashMap<Long> getFreeBytes(long[] files) {
        final LongHashMap<Long> result = new LongHashMap<>();
        synchronized (filesUtilization) {
            for (long file : files) {
                final FileUtilization fileUtilization = filesUtilization.get(file);
                result.put(file, (Long) (fileUtilization == null ? Long.MAX_VALUE : fileUtilization.getFreeBytes()));
            }
        }
        return result;
    }

    long getFileFreeBytes(final long fileAddress) {
        synchronized (filesUtilization) {
            final FileUtilization fileUtilization = filesUtilization.get(fileAddress);
            return fileUtilization == null ? Long.MAX_VALUE : fileUtilization.getFreeBytes();
        }
    }

    /**
     * Checks whether specified loggable is expired.
     *
     * @param loggable loggable to check.
     * @return true if the loggable is expired.
     */
    boolean isExpired(@NotNull final Loggable loggable) {
        final long fileAddress = log.getFileAddress(loggable.getAddress());
        synchronized (filesUtilization) {
            final FileUtilization fileUtilization = filesUtilization.get(fileAddress);
            return fileUtilization != null && fileUtilization.isExpired(loggable);
        }
    }

    boolean isExpired(long startAddress, int length) {
        final long fileAddress = log.getFileAddress(startAddress);
        synchronized (filesUtilization) {
            final FileUtilization fileUtilization = filesUtilization.get(fileAddress);
            return fileUtilization != null && fileUtilization.isExpired(startAddress, length);
        }
    }

    /**
     * Updates utilization profile with new expired loggables.
     *
     * @param loggables expired loggables.
     */
    void fetchExpiredLoggables(@NotNull final Iterable<Loggable> loggables) {
        synchronized (filesUtilization) {
            for (final Loggable loggable : loggables) {
                final long fileAddress = log.getFileAddress(loggable.getAddress());
                FileUtilization fileUtilization = filesUtilization.get(fileAddress);
                if (fileUtilization == null) {
                    fileUtilization = new FileUtilization();
                    filesUtilization.put(fileAddress, fileUtilization);
                }
                fileUtilization.fetchExpiredLoggable(loggable, fileUtilization.getFreeSpace());
            }
        }
    }

    void removeFile(final long fileAddress) {
        synchronized (filesUtilization) {
            filesUtilization.remove(fileAddress);
        }
    }

    void estimateTotalBytes() {
        // at first, estimate total bytes
        final long[] fileAddresses = log.getAllFileAddresses();
        final int filesCount = fileAddresses.length;
        int i = gc.getMinFileAge();
        totalBytes = filesCount > i ? (filesCount - i) * fileSize : 0;
        // then, estimate total free bytes
        long totalFreeBytes = 0;
        synchronized (filesUtilization) {
            for (; i < fileAddresses.length; ++i) {
                final FileUtilization fileUtilization = filesUtilization.get(fileAddresses[i]);
                totalFreeBytes += fileUtilization != null ? fileUtilization.getFreeBytes() : fileSize;
            }
        }
        this.totalFreeBytes = totalFreeBytes;
    }

    Long[] getFilesSortedByUtilization() {
        final long maxFreeBytes = fileSize * (long) gc.getMaximumFreeSpacePercent() / 100L;
        final long[] fileAddresses = log.getAllFileAddresses();
        final LongHashMap<Long> sparseFiles = new LongHashMap<>();
        synchronized (filesUtilization) {
            for (int i = gc.getMinFileAge(); i < fileAddresses.length; ++i) {
                final long file = fileAddresses[i];
                final FileUtilization fileUtilization = filesUtilization.get(file);
                if (fileUtilization == null) {
                    sparseFiles.put(file, (Long) Long.MAX_VALUE);
                } else {
                    final long freeBytes = fileUtilization.getFreeBytes();
                    if (freeBytes > maxFreeBytes) {
                        sparseFiles.put(file, (Long) freeBytes);
                    }
                }
            }
        }
        final Long[] result = new Long[sparseFiles.size()];
        for (int i = gc.getMinFileAge(), j = 0; i < fileAddresses.length; ++i) {
            final long file = fileAddresses[i];
            if (sparseFiles.containsKey(file)) {
                result[j++] = file;
            }
        }
        Arrays.sort(result, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                final long freeBytes1 = sparseFiles.get(o1);
                final long freeBytes2 = sparseFiles.get(o2);
                if (freeBytes1 == freeBytes2) {
                    return 0;
                }
                return freeBytes1 < freeBytes2 ? 1 : -1;
            }
        });
        return result;
    }
}