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
     * Executes whatever you need in parallel using all the available cores
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
     * Executes whatever you need in parallel using all the available cores
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
                    throw new RuntimeException("Error during original vector norms pre-calculation.", e);
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
