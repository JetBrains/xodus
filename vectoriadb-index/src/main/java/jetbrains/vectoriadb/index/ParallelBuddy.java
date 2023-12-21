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
