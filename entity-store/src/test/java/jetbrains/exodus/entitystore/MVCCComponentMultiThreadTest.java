package jetbrains.exodus.entitystore;

import org.junit.Test;

public class MVCCComponentMultiThreadTest {

    // Multi-thread tests

    // 1. Create two threads that change one key. Both threads write the same key but different values.
    // Make it so that at first one transaction rollbacks due to the fact that the third one then read
    // it in the pause. And then both committed because there was no reading in between.
    @Test
    public void twoThreadsUpdateSameKeyTest() {

    }


    // 2. Increment the same value by 12 threads. Repeat transactions as needed.
    // Check that the resulting value is correct.
    @Test
    public void modifySameValueMultipleThreadsTest() {

    }

    // 3. Increment the same value with 12 threads, but create a new transaction 1_000_000 times for each increment
    // in the thread. Check the execution time of all transactions in comparison with the execution time in one thread.
    @Test

    public void modifyValueWith12Threads1_000_100NewTransactionsOnEachIncrementTest() {

    }

    // 2.2 Add keys and delete in the same transaction. Check visibility before and after a commit in the current
    // thread and in a separate thread.
    @Test
    public void visibilityOnPutRemoveTest() {

    }
}
