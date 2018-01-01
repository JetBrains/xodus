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
package jetbrains.exodus.gc;

import jetbrains.exodus.bindings.LongBinding;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.core.dataStructures.hash.LongHashMap;
import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.env.*;
import jetbrains.exodus.log.*;
import jetbrains.exodus.tree.LongIterator;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.lang.ref.WeakReference;
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
    private final LongHashMap<MutableLong> filesUtilization; // file address -> number of free bytes
    private long totalBytes;
    private long totalFreeBytes;
    private volatile boolean isDirty;

    UtilizationProfile(@NotNull final EnvironmentImpl env, @NotNull final GarbageCollector gc) {
        this.env = env;
        this.gc = gc;
        log = env.getLog();
        fileSize = log.getFileSize() * 1024L;
        filesUtilization = new LongHashMap<>();
        log.addNewFileListener(new NewFileListener() {
            @Override
            @SuppressWarnings({"ConstantConditions"})
            public void fileCreated(long fileAddress) {
                synchronized (filesUtilization) {
                    filesUtilization.put(fileAddress, new MutableLong(0L));
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
        final EnvironmentConfig ec = env.getEnvironmentConfig();
        if (ec.getGcUtilizationFromScratch()) {
            computeUtilizationFromScratch();
        } else {
            final String storedUtilization = ec.getGcUtilizationFromFile();
            if (!storedUtilization.isEmpty()) {
                loadUtilizationFromFile(storedUtilization);
            } else {
                env.executeInReadonlyTransaction(new TransactionalExecutable() {
                    @Override
                    public void execute(@NotNull final Transaction txn) {
                        if (!env.storeExists(GarbageCollector.UTILIZATION_PROFILE_STORE_NAME, txn)) {
                            if (env.getAllStoreCount() == 0 && log.getNumberOfFiles() <= 1) {
                                clearUtilization();
                            } else {
                                computeUtilizationFromScratch();
                            }
                        } else {
                            final LongHashMap<MutableLong> filesUtilization = new LongHashMap<>();
                            final StoreImpl store = env.openStore(GarbageCollector.UTILIZATION_PROFILE_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES, txn);
                            try (Cursor cursor = store.openCursor(txn)) {
                                while (cursor.getNext()) {
                                    final long fileAddress = LongBinding.compressedEntryToLong(cursor.getKey());
                                    final long freeBytes = CompressedUnsignedLongByteIterable.getLong(cursor.getValue());
                                    filesUtilization.put(fileAddress, new MutableLong(freeBytes));
                                }
                            }
                            synchronized (UtilizationProfile.this.filesUtilization) {
                                UtilizationProfile.this.filesUtilization.clear();
                                UtilizationProfile.this.filesUtilization.putAll(filesUtilization);
                            }
                            estimateFreeBytesAndWakeGcIfNecessary();
                        }
                    }
                });
            }
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
            final List<Map.Entry<Long, MutableLong>> filesUtilization;
            synchronized (UtilizationProfile.this.filesUtilization) {
                filesUtilization = new ArrayList<>(UtilizationProfile.this.filesUtilization.entrySet());
            }
            for (final Map.Entry<Long, MutableLong> entry : filesUtilization) {
                store.put(txn,
                        LongBinding.longToCompressedEntry(entry.getKey()),
                        CompressedUnsignedLongByteIterable.getIterable(entry.getValue().value));
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
            final MutableLong freeBytes = filesUtilization.get(fileAddress);
            return freeBytes == null ? Long.MAX_VALUE : freeBytes.value;
        }
    }

    /**
     * Updates utilization profile with new expired loggables.
     *
     * @param loggables expired loggables.
     */
    void fetchExpiredLoggables(@NotNull final Iterable<ExpiredLoggableInfo> loggables) {
        long prevFileAddress = -1L;
        MutableLong prevFreeBytes = null;
        synchronized (filesUtilization) {
            for (final ExpiredLoggableInfo loggable : loggables) {
                final long fileAddress = log.getFileAddress(loggable.address);
                MutableLong freeBytes = prevFileAddress == fileAddress ? prevFreeBytes : filesUtilization.get(fileAddress);
                if (freeBytes == null) {
                    freeBytes = new MutableLong(0L);
                    filesUtilization.put(fileAddress, freeBytes);
                }
                freeBytes.value += loggable.length;
                prevFreeBytes = freeBytes;
                prevFileAddress = fileAddress;
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
                final MutableLong freeBytes = filesUtilization.get(fileAddresses[i]);
                totalFreeBytes += freeBytes != null ? freeBytes.value : fileSize;
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
                    final MutableLong freeBytes = filesUtilization.get(file);
                    if (freeBytes == null) {
                        fragmentedFiles.add(new Pair<>(file, fileSize));
                        totalFreeBytes[0] += fileSize;
                    } else {
                        final long freeBytesValue = freeBytes.value;
                        if (freeBytesValue > maxFreeBytes) {
                            fragmentedFiles.add(new Pair<>(file, freeBytesValue));
                        }
                        totalFreeBytes[0] += freeBytesValue;
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
     * Loads utilization profile from file.
     *
     * @param path external file with utilization info in the format as created by the {@code "-d"} option
     *             of the {@code Reflect} tool
     * @see EnvironmentConfig#setGcUtilizationFromFile(String)
     */
    public void loadUtilizationFromFile(@NotNull final String path) {
        gc.getCleaner().getJobProcessor().queueAt(new Job() {
            @Override
            protected void execute() throws Throwable {
                final LongHashMap<Long> usedSpace = new LongHashMap<>();
                try {
                    try (Scanner scanner = new Scanner(new File(path))) {
                        while (scanner.hasNextLong()) {
                            final long address = scanner.nextLong();
                            final Long usedBytes = scanner.nextLong();
                            usedSpace.put(address, usedBytes);
                        }
                    }
                } catch (Throwable t) {
                    GarbageCollector.loggingError("Failed to load utilization from " + path, t);
                }
                // if an error occurs during reading the file, then GC will be too pessimistic, i.e. it will clean
                // first the files which are missed in the utilization profile.
                setUtilization(usedSpace);
            }
        }, gc.getStartTime());
    }

    /**
     * Reloads utilization profile.
     */
    public void computeUtilizationFromScratch() {
        gc.getCleaner().getJobProcessor().queueAt(new ComputeUtilizationFromScratchJob(this), gc.getStartTime());
    }

    private void clearUtilization() {
        synchronized (filesUtilization) {
            filesUtilization.clear();
        }
    }

    private void setUtilization(LongHashMap<Long> usedSpace) {
        synchronized (filesUtilization) {
            filesUtilization.clear();
            for (final Map.Entry<Long, Long> entry : usedSpace.entrySet()) {
                filesUtilization.put(entry.getKey(), new MutableLong(fileSize - entry.getValue()));
            }
        }
        estimateFreeBytesAndWakeGcIfNecessary();
    }

    private void estimateFreeBytesAndWakeGcIfNecessary() {
        estimateTotalBytes();
        if (gc.isTooMuchFreeSpace()) {
            gc.wake();
        }
    }

    /**
     * Is used instead of {@linkplain Long} for saving free bytes per file in  order to update the value in-place, so
     * reducing number of lookups in the {@linkplain #filesUtilization LongHashMap}.
     */
    private static class MutableLong {

        private long value;

        MutableLong(final long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return Long.toString(value);
        }
    }

    private static class ComputeUtilizationFromScratchJob extends Job {

        private final WeakReference<UtilizationProfile> up;

        private ComputeUtilizationFromScratchJob(@NotNull final UtilizationProfile up) {
            this.up = new WeakReference<>(up);
        }

        @Override
        protected void execute() throws Throwable {
            final UtilizationProfile up = this.up.get();
            if (up == null) return;
            final LongHashMap<Long> usedSpace = new LongHashMap<>();
            final EnvironmentImpl env = up.env;
            env.executeInReadonlyTransaction(new TransactionalExecutable() {
                @Override
                public void execute(@NotNull Transaction txn) {
                    final Log log = up.log;
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
            up.setUtilization(usedSpace);
        }
    }
}