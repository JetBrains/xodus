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

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.core.dataStructures.LongArrayList;
import jetbrains.exodus.core.dataStructures.Priority;
import jetbrains.exodus.core.dataStructures.hash.IntHashMap;
import jetbrains.exodus.core.dataStructures.hash.LongSet;
import jetbrains.exodus.core.dataStructures.hash.PackedLongHashSet;
import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.core.execution.JobProcessorAdapter;
import jetbrains.exodus.env.*;
import jetbrains.exodus.io.RemoveBlockType;
import jetbrains.exodus.log.*;
import jetbrains.exodus.runtime.OOMGuard;
import jetbrains.exodus.util.DeferredIO;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

@SuppressWarnings({"ThisEscapedInObjectConstruction"})
public final class GarbageCollector {

    public static final String UTILIZATION_PROFILE_STORE_NAME = "exodus.gc.up";

    private static final Logger logger = LoggerFactory.getLogger(GarbageCollector.class);

    @NotNull
    private final EnvironmentImpl env;
    @NotNull
    private final EnvironmentConfig ec;
    @NotNull
    private final UtilizationProfile utilizationProfile;
    @NotNull
    private final LongSet pendingFilesToDelete;
    @NotNull
    private final ConcurrentLinkedQueue<Long> deletionQueue;
    @NotNull
    private final BackgroundCleaner cleaner;
    private volatile int newFiles; // number of new files appeared after last cleaning job
    @NotNull
    private final IntHashMap<StoreImpl> openStoresCache;
    private boolean useRegularTxn;

    public GarbageCollector(@NotNull final EnvironmentImpl env) {
        this.env = env;
        ec = env.getEnvironmentConfig();
        pendingFilesToDelete = new PackedLongHashSet();
        deletionQueue = new ConcurrentLinkedQueue<>();
        utilizationProfile = new UtilizationProfile(env, this);
        cleaner = new BackgroundCleaner(this);
        newFiles = ec.getGcFilesInterval() + 1;
        openStoresCache = new IntHashMap<>();
        env.getLog().addNewFileListener(new NewFileListener() {
            @Override
            public void fileCreated(long fileAddress) {
                final int newFiles = GarbageCollector.this.newFiles + 1;
                GarbageCollector.this.newFiles = newFiles;
                utilizationProfile.estimateTotalBytes();
                if (!cleaner.isCleaning() && newFiles > ec.getGcFilesInterval() && isTooMuchFreeSpace()) {
                    wake();
                }
            }
        });
    }

    public void clear() {
        utilizationProfile.clear();
        pendingFilesToDelete.clear();
        deletionQueue.clear();
        openStoresCache.clear();
        resetNewFiles();
    }

    @SuppressWarnings("unused")
    public void setCleanerJobProcessor(@NotNull final JobProcessorAdapter processor) {
        cleaner.getJobProcessor().queue(new Job() {
            @Override
            protected void execute() {
                cleaner.setJobProcessor(processor);
            }
        }, Priority.highest);
    }

    public void wake() {
        if (ec.isGcEnabled()) {
            env.executeTransactionSafeTask(new Runnable() {
                @Override
                public void run() {
                    cleaner.queueCleaningJob();
                }
            });
        }
    }

    void wakeAt(final long millis) {
        if (ec.isGcEnabled()) {
            cleaner.queueCleaningJobAt(millis);
        }
    }

    int getMaximumFreeSpacePercent() {
        return 100 - ec.getGcMinUtilization();
    }

    public void fetchExpiredLoggables(@NotNull final Iterable<ExpiredLoggableInfo> loggables) {
        utilizationProfile.fetchExpiredLoggables(loggables);
    }

    public long getFileFreeBytes(final long fileAddress) {
        return utilizationProfile.getFileFreeBytes(fileAddress);
    }

    public void suspend() {
        cleaner.suspend();
    }

    public void resume() {
        cleaner.resume();
    }

    public void finish() {
        cleaner.finish();
    }

    @NotNull
    public UtilizationProfile getUtilizationProfile() {
        return utilizationProfile;
    }

    boolean isTooMuchFreeSpace() {
        return utilizationProfile.totalFreeSpacePercent() > getMaximumFreeSpacePercent();
    }

    public /* public access is necessary to invoke the method from the Reflect class */
    boolean doCleanFile(final long fileAddress) {
        return doCleanFiles(Collections.singleton(fileAddress).iterator());
    }

    public static boolean isUtilizationProfile(@NotNull final String storeName) {
        return UTILIZATION_PROFILE_STORE_NAME.equals(storeName);
    }

    @NotNull
    BackgroundCleaner getCleaner() {
        return cleaner;
    }

    int getMinFileAge() {
        return ec.getGcFileMinAge();
    }

    void deletePendingFiles() {
        cleaner.checkThread();
        final LongArrayList filesToDelete = new LongArrayList();
        Long fileAddress;
        while ((fileAddress = deletionQueue.poll()) != null) {
            if (pendingFilesToDelete.remove(fileAddress)) {
                filesToDelete.add(fileAddress);
            }
        }
        if (!filesToDelete.isEmpty()) {
            // force flush and fsync in order to fix XD-249
            // in order to avoid data loss, it's necessary to make sure that any GC transaction is flushed
            // to underlying storage device before any file is deleted
            env.flushAndSync();
            for (final long file : filesToDelete.toArray()) {
                removeFile(file);
            }
        }
    }

    @NotNull
    EnvironmentImpl getEnvironment() {
        return env;
    }

    Log getLog() {
        return env.getLog();
    }

    long getStartTime() {
        return env.getCreated() + ec.getGcStartIn();
    }

    /**
     * Cleans fragmented files. It is expected that the files are sorted by utilization, i.e.
     * the first files are more fragmented. In order to avoid race conditions and synchronization issues,
     * this method should be called from the thread of background cleaner.
     *
     * @param fragmentedFiles fragmented files
     * @return {@code false} if there was unsuccessful attempt to clean a file (GC txn wasn't acquired or flushed)
     */
    boolean cleanFiles(@NotNull final Iterator<Long> fragmentedFiles) {
        cleaner.checkThread();
        return doCleanFiles(fragmentedFiles);
    }

    boolean isFileCleaned(final long file) {
        return pendingFilesToDelete.contains(file);
    }

    void resetNewFiles() {
        newFiles = 0;
    }

    void setUseRegularTxn(final boolean useRegularTxn) {
        this.useRegularTxn = useRegularTxn;
    }

    /**
     * For tests only!!!
     */
    void cleanWholeLog() {
        cleaner.cleanWholeLog();
    }

    /**
     * For tests only!!!
     */
    void testDeletePendingFiles() {
        final long[] files = pendingFilesToDelete.toLongArray();
        boolean aFileWasDeleted = false;
        for (final long file : files) {
            utilizationProfile.removeFile(file);
            getLog().removeFile(file, ec.getGcRenameFiles() ? RemoveBlockType.Rename : RemoveBlockType.Delete);
            aFileWasDeleted = true;
        }
        if (aFileWasDeleted) {
            pendingFilesToDelete.clear();
            utilizationProfile.estimateTotalBytes();
        }
    }

    static void loggingInfo(@NotNull final String message) {
        if (logger.isInfoEnabled()) {
            logger.info(message);
        }
    }

    static void loggingError(@NotNull final String message, @Nullable final Throwable t) {
        if (logger.isErrorEnabled()) {
            if (t == null) {
                logger.error(message);
            } else {
                logger.error(message, t);
            }
        }
    }

    private boolean doCleanFiles(@NotNull final Iterator<Long> fragmentedFiles) {
        // if there are no more files then even don't start a txn
        if (!fragmentedFiles.hasNext()) {
            return true;
        }
        final LongSet cleanedFiles = new PackedLongHashSet();
        final ReadWriteTransaction txn;
        try {
            final TransactionBase tx = useRegularTxn ? env.beginTransaction() : env.beginGCTransaction();
            // tx can be read-only, so we should manually finish it (see XD-667)
            if (tx.isReadonly()) {
                tx.abort();
                return false;
            }
            txn = (ReadWriteTransaction) tx;
        } catch (TransactionAcquireTimeoutException ignore) {
            return false;
        }
        final boolean isTxnExclusive = txn.isExclusive();
        try {
            final OOMGuard guard = new OOMGuard();
            final long started = System.currentTimeMillis();
            while (fragmentedFiles.hasNext()) {
                final long fileAddress = fragmentedFiles.next();
                cleanSingleFile(fileAddress, txn);
                cleanedFiles.add(fileAddress);
                if (!isTxnExclusive) {
                    break; // do not process more than one file in a non-exclusive txn
                }
                if (started + ec.getGcTransactionTimeout() < System.currentTimeMillis()) {
                    break; // break by timeout
                }
                if (guard.isItCloseToOOM()) {
                    break; // break because of the risk of OutOfMemoryError
                }
            }
            if (!txn.forceFlush()) {
                // paranoiac check
                if (isTxnExclusive) {
                    throw new ExodusException("Can't be: exclusive txn should be successfully flushed");
                }
                return false;
            }
        } catch (Throwable e) {
            throw ExodusException.toExodusException(e);
        } finally {
            txn.abort();
        }
        if (!cleanedFiles.isEmpty()) {
            for (final Long file : cleanedFiles) {
                pendingFilesToDelete.add(file);
                utilizationProfile.removeFile(file);
            }
            utilizationProfile.estimateTotalBytes();
            env.executeTransactionSafeTask(new Runnable() {
                @Override
                public void run() {
                    final int filesDeletionDelay = ec.getGcFilesDeletionDelay();
                    if (filesDeletionDelay == 0) {
                        for (final Long file : cleanedFiles) {
                            deletionQueue.offer(file);
                        }
                    } else {
                        DeferredIO.getJobProcessor().queueIn(new Job() {
                            @Override
                            protected void execute() {
                                for (final Long file : cleanedFiles) {
                                    deletionQueue.offer(file);
                                }
                            }
                        }, filesDeletionDelay);
                    }
                }
            });
        }
        return true;
    }

    /**
     * @param fileAddress address of the file to clean
     * @param txn         transaction
     */
    private void cleanSingleFile(final long fileAddress, @NotNull final ReadWriteTransaction txn) {
        // the file can be already cleaned
        if (isFileCleaned(fileAddress)) {
            throw new ExodusException("Attempt to clean already cleaned file");
        }
        loggingInfo("start cleanFile(" + env.getLocation() + File.separatorChar + LogUtil.getLogFilename(fileAddress) + ')');
        final Log log = getLog();
        if (logger.isDebugEnabled()) {
            final long high = log.getHighAddress();
            final long highFile = log.getHighFileAddress();
            logger.debug(String.format(
                "Cleaner acquired txn when log high address was: %d (%s@%d) when cleaning file %s",
                high, LogUtil.getLogFilename(highFile), high - highFile, LogUtil.getLogFilename(fileAddress)
            ));
        }
        try {
            final long nextFileAddress = fileAddress + log.getFileLengthBound();
            final Iterator<RandomAccessLoggable> loggables = log.getLoggableIterator(fileAddress);
            while (loggables.hasNext()) {
                final RandomAccessLoggable loggable = loggables.next();
                if (loggable == null || loggable.getAddress() >= nextFileAddress) {
                    break;
                }
                final int structureId = loggable.getStructureId();
                if (structureId != Loggable.NO_STRUCTURE_ID && structureId != EnvironmentImpl.META_TREE_ID) {
                    StoreImpl store = openStoresCache.get(structureId);
                    if (store == null) {
                        // TODO: remove openStoresCache when txn.openStoreByStructureId() is fast enough (XD-381)
                        store = txn.openStoreByStructureId(structureId);
                        openStoresCache.put(structureId, store);
                    }
                    store.reclaim(txn, loggable, loggables);
                }
            }
        } catch (Throwable e) {
            logger.error("cleanFile(" + LogUtil.getLogFilename(fileAddress) + ')', e);
            throw e;
        }
    }

    private void removeFile(final long file) {
        getLog().removeFile(file, ec.getGcRenameFiles() ? RemoveBlockType.Rename : RemoveBlockType.Delete);
    }
}
