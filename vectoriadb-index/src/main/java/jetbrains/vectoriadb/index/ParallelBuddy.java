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


    public int numWorkers() {
        return numWorkers;
    }

    public void run(
            @NotNull String procedureName,
            int totalSize,
            @NotNull ProgressTracker progressTracker,
            @NotNull ParallelExecution.Action action
    ) {
        ParallelExecution.execute(procedureName, totalSize, numWorkers, executors, progressTracker, action);
    }

    public void run(
            @NotNull String procedureName,
            int totalSize,
            @NotNull ProgressTracker progressTracker,
            @NotNull ParallelExecution.Init init,
            @NotNull ParallelExecution.Action action
    ) {
        ParallelExecution.execute(procedureName, totalSize, numWorkers, executors, progressTracker, init, action);
    }

    @Override
    public void close() {
        executors.close();
    }
}
