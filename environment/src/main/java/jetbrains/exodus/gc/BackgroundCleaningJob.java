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

import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.env.EnvironmentImpl;
import jetbrains.exodus.log.Log;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
        if (gc != null && canContinue()) {
            final EnvironmentImpl env = gc.getEnvironment();
            final int gcStartIn = env.getEnvironmentConfig().getGcStartIn();
            if (gcStartIn != 0) {
                final long minTimeToInvokeCleaner = gcStartIn + env.getCreated();
                if (minTimeToInvokeCleaner > System.currentTimeMillis()) {
                    gc.wakeAt(minTimeToInvokeCleaner);
                    return;
                }
            }
            final Log log = env.getLog();
            if (gc.getMinFileAge() < log.getNumberOfFiles()) {
                gc.getCleaner().setCleaning(true);
                try {
                    doCleanLog(log, gc);
                } finally {
                    gc.getCleaner().setCleaning(false);
                }
            }
        }
    }

    private void doCleanLog(@NotNull final Log log, @NotNull final GarbageCollector gc) {
        GarbageCollector.loggingInfo("Starting background cleaner loop for " + log.getLocation());
        final int newFiles = gc.getNewFiles();
        final Long[] sparseFiles = gc.getUtilizationProfile().getFilesSortedByUtilization();
        for (int i = 0; i < sparseFiles.length && canContinue(); ++i) {
            // reset new files count before each cleaned file to prevent queueing of the
            // next cleaning job before this one is not finished
            gc.resetNewFiles();
            final long file = sparseFiles[i];
            if (i > newFiles) {
                if (!cleanFile(gc, file)) {
                    break;
                }
            } else {
                for (int j = 0; j < 4 && !cleanFile(gc, file) && canContinue(); ++j) {
                    Thread.yield();
                }
            }
        }
        gc.resetNewFiles();
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
        gc.deletePendingFiles();
        final BackgroundCleaner cleaner = gc.getCleaner();
        return !cleaner.isSuspended() && !cleaner.isFinished() &&
                gc.getEnvironment().getEnvironmentConfig().isGcEnabled() && gc.isTooMuchFreeSpace();
    }
}
