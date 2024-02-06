package jetbrains.exodus.testutil

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

fun runInParallel(concurrencyLevel: Int, taskCount: Int = 1, action: (Int) -> Unit) {
    val executor = Executors.newFixedThreadPool(concurrencyLevel)
    repeat(taskCount) { i ->
        executor.submit {
            action(i)
        }
    }
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.MINUTES)
}