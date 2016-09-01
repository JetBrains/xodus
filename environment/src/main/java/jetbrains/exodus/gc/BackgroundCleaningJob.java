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

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.EnvironmentImpl;
import jetbrains.exodus.log.Log;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

@SuppressWarnings("NullableProblems")
final class BackgroundCleaningJob extends Job {

    @Nullable
    private GarbageCollector gc;

    BackgroundCleaningJob(@NotNull final GarbageCollector gc) {
        this.gc = gc;
    }

    @Override
    public String getName() {
        return "Background cleaner";
    }

    @Override
    public String getGroup() {
        return gc == null ? "<finished>" : gc.getEnvironment().getLocation();
    }

    /**
     * Cancels job so that it never will be executed again.
     */
    void cancel() {
        gc = null;
        setProcessor(null);
    }

    @Override
    protected void execute() throws Throwable {
        final GarbageCollector gc = this.gc;
        if (gc == null) {
            return;
        }
        final BackgroundCleaner cleaner = gc.getCleaner();
        if (canContinue()) {
            final EnvironmentImpl env = gc.getEnvironment();
            final EnvironmentConfig ec = env.getEnvironmentConfig();
            final int gcStartIn = ec.getGcStartIn();
            if (gcStartIn != 0) {
                final long minTimeToInvokeCleaner = gcStartIn + env.getCreated();
                if (minTimeToInvokeCleaner > System.currentTimeMillis()) {
                    gc.wakeAt(minTimeToInvokeCleaner);
                    return;
                }
            }
            final Log log = env.getLog();
            if (gc.getMinFileAge() < log.getNumberOfFiles()) {
                cleaner.setCleaning(true);
                try {
                    doCleanLog(log, gc);
                    if (gc.isTooMuchFreeSpace()) {
                        final int gcRunPeriod = ec.getGcRunPeriod();
                        if (gcRunPeriod > 0) {
                            gc.wakeAt(System.currentTimeMillis() + gcRunPeriod);
                        }
                    }
                } finally {
                    cleaner.setCleaning(false);
                }
            }
        }
        // XD-446: if we stopped cleaning cycle due to background cleaner job processor has changed then re-queue the job to another processor
        if (!cleaner.isCurrentThread()) {
            gc.wake();
        }
    }

    private void doCleanLog(@NotNull final Log log, @NotNull final GarbageCollector gc) {
        GarbageCollector.loggingInfo("Starting background cleaner loop for " + log.getLocation());

        final EnvironmentImpl env = gc.getEnvironment();
        final UtilizationProfile up = gc.getUtilizationProfile();
        final int newFiles = gc.getNewFiles();
        final EnvironmentConfig ec = env.getEnvironmentConfig();
        final long newBytesThreshold = newFiles * ec.getLogFileSize() * 1024L;
        long initialHighAddress = log.getHighAddress();
        Long[] sparseFiles = up.getFilesSortedByUtilization();

        final long loopStart = System.currentTimeMillis();

        for (int i = 0; i < sparseFiles.length && canContinue() && loopStart + ec.getGcRunPeriod() > System.currentTimeMillis(); ) {
            if (cleanFile(gc, sparseFiles[i])) {
                // reset new files count before each cleaned file to prevent queueing of the
                // next cleaning job before this one is not finished
                gc.resetNewFiles();
                ++i;
            }
            final long newHighAddress = log.getHighAddress();
            if (newHighAddress > initialHighAddress + newBytesThreshold) {
                initialHighAddress = newHighAddress;
                sparseFiles = Arrays.copyOf(up.getFilesSortedByUtilization(), sparseFiles.length - i);
                i = 0;
            }
        }

        gc.resetNewFiles();
        up.setDirty(true);

        GarbageCollector.loggingInfo("Finished background cleaner loop for " + log.getLocation());
    }

    /**
     * We need this synchronized method in order to provide correctness of  {@link BackgroundCleaner#suspend()}.
     */
    private synchronized boolean cleanFile(@NotNull final GarbageCollector gc, final long file) {
        return gc.cleanFile(file);
    }

    private boolean canContinue() {
        final GarbageCollector gc = this.gc;
        if (gc == null) {
            return false;
        }
        final BackgroundCleaner cleaner = gc.getCleaner();
        try {
            gc.deletePendingFiles();
        } catch (ExodusException e) {
            if (!cleaner.isCurrentThread()) {
                return false;
            }
            throw e;
        }
        if (cleaner.isSuspended() || cleaner.isFinished()) {
            return false;
        }
        final EnvironmentConfig ec = gc.getEnvironment().getEnvironmentConfig();
        return ec.isGcEnabled() && !ec.getEnvIsReadonly() && gc.isTooMuchFreeSpace();
    }
}
