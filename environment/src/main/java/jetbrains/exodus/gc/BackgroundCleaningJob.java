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

import jetbrains.exodus.core.execution.Job;
import jetbrains.exodus.env.EnvironmentConfig;
import jetbrains.exodus.env.EnvironmentImpl;
import jetbrains.exodus.log.Log;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;

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
        if (!cleaner.isCurrentThread()) {
            return;
        }
        try {
            if (canContinue()) {
                final long minTimeToInvokeCleaner = gc.getStartTime();
                if (minTimeToInvokeCleaner > System.currentTimeMillis()) {
                    gc.wakeAt(minTimeToInvokeCleaner);
                    return;
                }
                final EnvironmentImpl env = gc.getEnvironment();
                final EnvironmentConfig ec = env.getEnvironmentConfig();
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
        } finally {
            gc.deletePendingFiles();
        }
    }

    private void doCleanLog(@NotNull final Log log, @NotNull final GarbageCollector gc) {
        GarbageCollector.loggingInfo("Starting background cleaner loop for " + log.getLocation());

        final EnvironmentImpl env = gc.getEnvironment();
        final UtilizationProfile up = gc.getUtilizationProfile();
        final long highFile = log.getHighFileAddress();
        final long loopStart = System.currentTimeMillis();
        final int gcRunPeriod = env.getEnvironmentConfig().getGcRunPeriod();

        try {
            do {
                final Iterator<Long> fragmentedFiles = up.getFilesSortedByUtilization(highFile);
                if (!fragmentedFiles.hasNext()) {
                    return;
                }
                if (cleanFiles(gc, fragmentedFiles)) {
                    break;
                }
                Thread.yield();
            } while (canContinue() && loopStart + gcRunPeriod > System.currentTimeMillis());
            gc.setUseRegularTxn(true);
            try {
                while (canContinue() && loopStart + gcRunPeriod > System.currentTimeMillis()) {
                    final Iterator<Long> fragmentedFiles = up.getFilesSortedByUtilization(highFile);
                    if (!fragmentedFiles.hasNext() || !cleanFiles(gc, fragmentedFiles)) {
                        break;
                    }
                }
            } finally {
                gc.setUseRegularTxn(false);
            }
        } finally {
            gc.resetNewFiles();
            up.estimateTotalBytes();
            up.setDirty(true);
            GarbageCollector.loggingInfo("Finished background cleaner loop for " + log.getLocation());
        }
    }

    /**
     * We need this synchronized method in order to provide correctness of  {@link BackgroundCleaner#suspend()}.
     */
    private synchronized boolean cleanFiles(@NotNull final GarbageCollector gc,
                                            @NotNull final Iterator<Long> fragmentedFiles) {
        return gc.cleanFiles(fragmentedFiles);
    }

    private boolean canContinue() {
        final GarbageCollector gc = this.gc;
        if (gc == null) {
            return false;
        }
        final BackgroundCleaner cleaner = gc.getCleaner();
        if (cleaner.isSuspended() || cleaner.isFinished()) {
            return false;
        }
        final EnvironmentConfig ec = gc.getEnvironment().getEnvironmentConfig();
        return ec.isGcEnabled() && !ec.getEnvIsReadonly() && gc.isTooMuchFreeSpace();
    }
}
