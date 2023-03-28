package jetbrains.exodus.entitystore;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.core.dataStructures.Pair;
import jetbrains.exodus.newLogConcept.MVCC.MVCCDataStructure;
import jetbrains.exodus.newLogConcept.Transaction.Transaction;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static jetbrains.exodus.entitystore.TestBase.logger;

public class MVCCComponentMultiThreadTest {

    // Multi-thread tests

    // 1. Create two threads that change one key. Both threads write the same key but different values.
    // Make it so that at first one transaction rollbacks due to the fact that the third one then read
    // it in the pause. And then both committed because there was no reading in between.
    @Test
    public void twoThreadsUpdateSameKeyTest() {

    }


    // 2. Increment the same value by 120 threads. Repeat transactions as needed.
    // Check that the resulting value is correct.
    @Test
    public void modifySameValueMultipleThreadsTest() throws ExecutionException, InterruptedException {
        ExecutorService service = Executors.newCachedThreadPool();
        Map<String, String> keyValTransactions = new HashMap<>();
        var mvccComponent = new MVCCDataStructure();

        String keyString = "key-" + (int) (Math.random() * 100000);
        AtomicLong value = new AtomicLong(1000);
        keyValTransactions.put(keyString, String.valueOf(value));

        for (int i = 0; i < 120; i++) {
            var th = service.submit(() -> {
                Transaction writeTransaction = mvccComponent.startWriteTransaction();
                // check record is null before the commit
                mvccComponent.put(writeTransaction, StringBinding.stringToEntry(keyString),
                        StringBinding.stringToEntry(String.valueOf(value)));

                Assert.assertEquals(writeTransaction.getSnapshotId(), writeTransaction.getOperationLinkList().get(0).getTxId());
                mvccComponent.commitTransaction(writeTransaction);
                value.getAndIncrement();

            });
            th.get();
        }
        Assert.assertEquals(value.get(), 1120);
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
