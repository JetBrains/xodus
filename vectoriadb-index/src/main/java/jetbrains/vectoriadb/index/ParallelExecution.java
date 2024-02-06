/*
 * Copyright ${inceptionYear} - ${year} ${owner}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.vectoriadb.index;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class ParallelExecution {
    public static int availableCores() {
        return Runtime.getRuntime().availableProcessors();
    }

    public static int assignmentSize(int itemsToProcessCount, int numWorkers) {
        return (itemsToProcessCount + numWorkers - 1) / numWorkers;
    }

    public static ExecutorService makeExecutors(int cores, @NotNull String threadWorkerName) {
        return Executors.newFixedThreadPool(cores, r -> {
            var thread = new Thread(r);
            thread.setName(threadWorkerName + thread.threadId());
            return thread;
        });
    }

    /**
     * Executes whatever you need in parallel using all the available cores.
     * Splits totalSize evenly among the workers.
     * */
    public static void splitEvenly(
            @NotNull String procedureName,
            int totalSize,
            int cores,
            ExecutorService executors,
            @NotNull ProgressTracker progressTracker,
            @NotNull Action action
    ) {
        splitEvenly(procedureName, totalSize, cores, executors, progressTracker, (_) -> {}, action);
    }

    /**
     * Executes whatever you need in parallel using all the available cores.
     * Splits totalSize evenly among the workers.
     * */
    public static void splitEvenly(
            @NotNull String procedureName,
            int totalSize,
            int cores,
            ExecutorService executors,
            @NotNull ProgressTracker progressTracker,
            @NotNull Init init,
            @NotNull Action action
    ) {
        progressTracker.pushPhase(procedureName);

        try {
            var numTasks = Math.min(cores, totalSize);
            var assignmentSize = ParallelExecution.assignmentSize(totalSize, numTasks);
            var futures = new Future[numTasks];
            var mtProgressTracker = new BoundedMTProgressTrackerFactory(numTasks, progressTracker);

            for (int i = 0; i < numTasks; i++) {
                var start = i * assignmentSize;
                var end = Math.min(start + assignmentSize, totalSize);
                var workerId = i;

                futures[workerId] = executors.submit(() -> {
                    try (var localTracker = mtProgressTracker.createThreadLocalTracker(workerId)) {
                        init.invoke(workerId);
                        var localSize = end - start;
                        for (int k = 0; k < localSize; k++) {
                            var itemIdx = start + k;
                            action.invoke(workerId, itemIdx);
                            localTracker.progress(k * 100.0 / localSize);
                        }
                    }
                });
            }

            for (var future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(STR."Error during \{procedureName} parallel execution: \{e.getMessage()}", e);
                }
            }
        } finally {
            progressTracker.pullPhase();
        }
    }

    /**
     * Executes whatever you need in parallel using all the available cores.
     * All the workers process all the items out of totalSize.
     * */
    public static void execute(
            @NotNull String procedureName,
            int totalSize,
            int cores,
            ExecutorService executors,
            @NotNull ProgressTracker progressTracker,
            @NotNull Action action
    ) {
        execute(procedureName, totalSize, cores, executors, progressTracker, (_) -> {}, action);
    }

    /**
     * Executes whatever you need in parallel using all the available cores.
     * All the workers process all the items out of totalSize.
     * */
    public static void execute(
            @NotNull String procedureName,
            int totalSize,
            int cores,
            ExecutorService executors,
            @NotNull ProgressTracker progressTracker,
            @NotNull Init init,
            @NotNull Action action
    ) {
        progressTracker.pushPhase(procedureName);

        try {
            var futures = new Future[cores];
            var mtProgressTracker = new BoundedMTProgressTrackerFactory(cores, progressTracker);

            for (int i = 0; i < cores; i++) {
                var workerId = i;

                futures[workerId] = executors.submit(() -> {
                    try (var localTracker = mtProgressTracker.createThreadLocalTracker(workerId)) {
                        init.invoke(workerId);
                        for (int itemIdx = 0; itemIdx < totalSize; itemIdx++) {
                            action.invoke(workerId, itemIdx);
                            localTracker.progress(itemIdx * 100.0 / totalSize);
                        }
                    }
                });
            }

            for (var future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(STR."Error during \{procedureName} parallel execution: \{e.getMessage()}", e);
                }
            }
        } finally {
            progressTracker.pullPhase();
        }
    }

    public interface Init {
        void invoke(int workerIdx);
    }

    public interface Action {
        void invoke(int workerIdx, int itemIdx);
    }
}
