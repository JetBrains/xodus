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

import java.util.concurrent.ExecutorService;

public class ParallelBuddy implements AutoCloseable {

    private final int numWorkers;

    @NotNull
    private final ExecutorService executors;

    public ParallelBuddy(int numWorkers, @NotNull String workerName) {
        this.numWorkers = numWorkers;
        this.executors = ParallelExecution.makeExecutors(numWorkers, workerName);
    }

    public ParallelBuddy(String workerName) {
        this(ParallelExecution.availableCores(), workerName);
    }


    public int numWorkers() {
        return numWorkers;
    }

    /**
     * Executes whatever you need in parallel using numWorkers workers.
     * Splits totalSize evenly among the workers.
     * */
    public void runSplitEvenly(
            @NotNull String procedureName,
            int totalSize,
            @NotNull ProgressTracker progressTracker,
            @NotNull ParallelExecution.Action action
    ) {
        ParallelExecution.splitEvenly(procedureName, totalSize, numWorkers, executors, progressTracker, action);
    }

    /**
     * Executes whatever you need in parallel using numWorkers workers.
     * Splits totalSize evenly among the workers.
     * */
    public void runSplitEvenly(
            @NotNull String procedureName,
            int totalSize,
            @NotNull ProgressTracker progressTracker,
            @NotNull ParallelExecution.Init init,
            @NotNull ParallelExecution.Action action
    ) {
        ParallelExecution.splitEvenly(procedureName, totalSize, numWorkers, executors, progressTracker, init, action);
    }

    /**
     * Executes whatever you need in parallel using the numWorkers workers.
     * All the workers process all the items out of totalSize.
     * */
    public void run(
            @NotNull String procedureName,
            int totalSize,
            @NotNull ProgressTracker progressTracker,
            @NotNull ParallelExecution.Action action
    ) {
        ParallelExecution.execute(procedureName, totalSize, numWorkers, executors, progressTracker, action);
    }

    @Override
    public void close() {
        executors.close();
    }
}
