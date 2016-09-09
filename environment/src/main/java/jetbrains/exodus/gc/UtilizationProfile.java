/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.Priority;
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
    private volatile boolean isDirty;

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

    void clear() {
        synchronized (filesUtilization) {
            filesUtilization.clear();
        }
        estimateTotalBytes();
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
     * Saves utilization profile in internal store within specified transaction.
     */
    public void save(@NotNull final Transaction txn) {
        if (isDirty) {
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
    }

    public boolean isDirty() {
        return isDirty;
    }

    public void setDirty(boolean dirty) {
        isDirty = dirty;
    }

    int totalFreeSpacePercent() {
        final long totalBytes = this.totalBytes;
        return (int) (totalBytes == 0 ? 0 : ((totalFreeBytes * 100L) / totalBytes));
    }

    public int totalUtilizationPercent() {
        return 100 - totalFreeSpacePercent();
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
    void fetchExpiredLoggables(@NotNull final Iterable<ExpiredLoggableInfo> loggables) {
        synchronized (filesUtilization) {
            for (final ExpiredLoggableInfo loggable : loggables) {
                final long fileAddress = log.getFileAddress(loggable.address);
                FileUtilization fileUtilization = filesUtilization.get(fileAddress);
                if (fileUtilization == null) {
                    fileUtilization = new FileUtilization();
                    filesUtilization.put(fileAddress, fileUtilization);
                }
                fileUtilization.fetchExpiredLoggable(loggable,
                        env.getEnvironmentConfig().getGcUseExpirationChecker() ? fileUtilization.getFreeSpace() : null);
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

    Iterator<Long> getFilesSortedByUtilization(final long highFile) {
        final long[] fileAddresses = log.getAllFileAddresses();
        final long maxFreeBytes = fileSize * (long) gc.getMaximumFreeSpacePercent() / 100L;
        final PriorityQueue<Pair<Long, Long>> fragmentedFiles = new PriorityQueue<>(10, new Comparator<Pair<Long, Long>>() {
            @Override
            public int compare(Pair<Long, Long> leftPair, Pair<Long, Long> rightPair) {
                final long leftFreeBytes = leftPair.getSecond();
                final long rightFreeBytes = rightPair.getSecond();
                if (leftFreeBytes == rightFreeBytes) {
                    return 0;
                }
                return leftFreeBytes > rightFreeBytes ? -1 : 1;
            }
        });
        final long[] totalCleanableBytes = {0};
        final long[] totalFreeBytes = {0};
        synchronized (filesUtilization) {
            for (int i = gc.getMinFileAge(); i < fileAddresses.length; ++i) {
                final long file = fileAddresses[i];
                if (file < highFile && !gc.isFileCleaned(file)) {
                    totalCleanableBytes[0] += fileSize;
                    final FileUtilization fileUtilization = filesUtilization.get(file);
                    if (fileUtilization == null) {
                        fragmentedFiles.add(new Pair<>(file, fileSize));
                        totalFreeBytes[0] += fileSize;
                    } else {
                        final long freeBytes = fileUtilization.getFreeBytes();
                        if (freeBytes > maxFreeBytes) {
                            fragmentedFiles.add(new Pair<>(file, freeBytes));
                        }
                        totalFreeBytes[0] += freeBytes;
                    }
                }
            }
        }
        return new Iterator<Long>() {

            @Override
            public boolean hasNext() {
                return !fragmentedFiles.isEmpty() &&
                        totalFreeBytes[0] > totalCleanableBytes[0] * gc.getMaximumFreeSpacePercent() / 100L;
            }

            @Override
            public Long next() {
                final Pair<Long, Long> pair = fragmentedFiles.poll();
                totalFreeBytes[0] -= pair.getSecond();
                return pair.getFirst();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove");
            }
        };
    }

    /**
     * Reloads utilization profile.
     */
    private void computeUtilizationFromScratch() {
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
}