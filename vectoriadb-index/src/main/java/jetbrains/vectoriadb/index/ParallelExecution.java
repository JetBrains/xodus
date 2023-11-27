package jetbrains.vectoriadb.index;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class ParallelExecution {
    public static int availableCores(long itemsToProcessCount) {
        return (int) Math.min(Runtime.getRuntime().availableProcessors(), itemsToProcessCount);
    }

    public static long assignmentSize(long itemsToProcessCount, int cores) {
        return (itemsToProcessCount + cores - 1) / cores;
    }

    /**
     * Executes whatever you need in parallel using all the available cores
     * */
    public static void execute(
            @NotNull String procedureName,
            @NotNull String threadWorkerName,
            long totalSize,
            @NotNull ProgressTracker progressTracker,
            @NotNull Action action
    ) {
        progressTracker.pushPhase(procedureName);

        var cores = ParallelExecution.availableCores(totalSize);
        try (var executors = Executors.newFixedThreadPool(cores, r -> {
            var thread = new Thread(r);
            thread.setName(threadWorkerName + thread.threadId());
            return thread;
        })) {
            var assignmentSize = ParallelExecution.assignmentSize(totalSize, cores);
            var futures = new Future[cores];
            var mtProgressTracker = new BoundedMTProgressTrackerFactory(cores, progressTracker);

            for (int i = 0; i < cores; i++) {
                var start = i * assignmentSize;
                var end = Math.min(start + assignmentSize, totalSize);
                var id = i;

                futures[i] = executors.submit(() -> {
                    try (var localTracker = mtProgressTracker.createThreadLocalTracker(id)) {
                        var localSize = end - start;
                        for (long k = 0; k < localSize; k++) {
                            long itemIdx = start + k;
                            action.invoke(itemIdx);
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

    public interface Action {
        void invoke(long itemIdx);
    }
}
