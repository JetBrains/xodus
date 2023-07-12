package jetbrains.exodus.entitystore;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.newLogConcept.MVCC.MVCCDataStructure;
import jetbrains.exodus.newLogConcept.transaction.Transaction;
import jetbrains.exodus.newLogConcept.transaction.TransactionState;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Long.parseLong;
import static jetbrains.exodus.entitystore.TestBase.logger;


public class MVCCComponentMultiThreadTest {

    // Multi-thread tests

    // 1. Create two threads that change one key. Both threads write the same key but different values.
    // Make it so that at first one transaction rollbacks due to the fact that the third one then read
    // it in the pause. And then both committed because there was no reading in between.
    @Test
    public void twoThreadsUpdateSameKeyTest() throws ExecutionException, InterruptedException {
        ExecutorService service = Executors.newCachedThreadPool();
        var mvccComponent = new MVCCDataStructure();
        String keyString = "key-100000";
        AtomicLong txId2 = new AtomicLong();

        var th = service.submit(() -> {
            Transaction writeTransaction = mvccComponent.startWriteTransaction();
            var txId = writeTransaction.getSnapshotId();
            var th2 = service.submit(() -> {
                Transaction writeTransaction2 = mvccComponent.startWriteTransaction();
                txId2.set(writeTransaction2.getSnapshotId());
                // check record is null before the commit
                mvccComponent.put(writeTransaction2, StringBinding.stringToEntry(keyString),
                        StringBinding.stringToEntry("1000"));
                try {
                    mvccComponent.commitTransaction(writeTransaction2);
                } catch (ExecutionException | InterruptedException e) {
                    throw new ExodusException(e);
                }
            });
            try {
                th2.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
            mvccComponent.put(writeTransaction, StringBinding.stringToEntry(keyString),
                    StringBinding.stringToEntry("2000"));

            try {
                try {
                    mvccComponent.commitTransaction(writeTransaction);
                } catch (ExecutionException | InterruptedException e) {
                    throw new ExodusException(e);
                }
            } catch (ExodusException ignored) {
            }
            Assert.assertTrue(mvccComponent.transactionsGCMap.containsKey(txId) &&
                    mvccComponent.transactionsGCMap.get(txId).stateWrapper.state == TransactionState.REVERTED.get());
        });
        th.get();


        var th3 = service.submit(() -> {
            Transaction readTransaction = mvccComponent.startReadTransaction();
            ByteIterable record = mvccComponent.read(readTransaction,
                    StringBinding.stringToEntry("key-100000"));
            logger.debug("Assert key: key-100000");
            Assert.assertEquals("1000", StringBinding.entryToString(record));

        });
        th3.get();
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

        //measuring elapsed time using System.nanoTime
        long startTime = System.nanoTime();

        for (int i = 0; i < 120; i++) {
            var th = service.submit(() -> {
                Transaction writeTransaction = mvccComponent.startWriteTransaction();
                // check record is null before the commit
                mvccComponent.put(writeTransaction, StringBinding.stringToEntry(keyString),
                        StringBinding.stringToEntry(String.valueOf(value)));

                Assert.assertEquals(writeTransaction.getSnapshotId(), writeTransaction.getOperationLinkList().get(0).getTxId());
                try {
                    mvccComponent.commitTransaction(writeTransaction);
                } catch (ExecutionException | InterruptedException e) {
                    throw new ExodusException(e);
                }
            });
            value.getAndIncrement();
            th.get();
        }

        System.out.println("Execution time in millis: " + (System.nanoTime() - startTime));
        Assert.assertEquals(value.get(), 1120);
    }

    // 3. Increment the same value with 120 threads, but create a new transaction 1_000_000 times for each increment
    // in the thread.
    @Test
    public void incrementWith120ThreadsTest() throws InterruptedException, ExecutionException {
        final int THREAD_COUNT = 120;
        final int TRANSACTION_COUNT = 100_000;
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        var mvccComponent = new MVCCDataStructure();

        String keyString = "key-1000";
        String value = "1";

        long startTime = System.currentTimeMillis();

        var th2 = executorService.submit(() -> {
            Transaction writeTransaction = mvccComponent.startWriteTransaction();
            // check record is null before the commit
            mvccComponent.put(writeTransaction, StringBinding.stringToEntry(keyString), StringBinding.stringToEntry(value));
            try {
                mvccComponent.commitTransaction(writeTransaction);
            } catch (ExecutionException | InterruptedException e) {
                throw new ExodusException(e);
            }
        });
        try {
            th2.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        // Execute transactions in multiple threads
        Future<?>[] futures = new Future[THREAD_COUNT];
        for (int i = 0; i < THREAD_COUNT; i++) {
            futures[i] = executorService.submit(() -> {
                String result;
                var th = executorService.submit(() -> {
                    Transaction readTransaction = mvccComponent.startReadTransaction();
                    // check record is null before the commit
                    return StringBinding.entryToString(mvccComponent
                            .read(readTransaction, StringBinding.stringToEntry(keyString)));
                });
                try {
                    result = th.get();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
                var newValue  = new AtomicLong(parseLong(result)).incrementAndGet();
                for (int j = 0; j < TRANSACTION_COUNT; j++) {

                Transaction writeTransaction = mvccComponent.startWriteTransaction();
                    // check record is null before the commit
                    mvccComponent.put(writeTransaction, StringBinding.stringToEntry(keyString),
                            StringBinding.stringToEntry(String.valueOf(newValue)));

                    try {
                        mvccComponent.commitTransaction(writeTransaction);
                    } catch (ExecutionException | InterruptedException e) {
                        throw new ExodusException(e);
                    }
                }
            });
            futures[i].get();
        }

//        // Wait for all threads to complete
//        for (int i = 0; i < THREAD_COUNT; i++) {
//            futures[i].get();
//        }

        var th = executorService.submit(() -> {
            Transaction readTransaction = mvccComponent.startReadTransaction();
            // check record is null before the commit
            var result = mvccComponent.read(readTransaction, StringBinding.stringToEntry(keyString));
            Assert.assertEquals("120", StringBinding.entryToString(result));

        });
        th.get();

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Total execution time with " + THREAD_COUNT + " threads: " + totalTime + "ms");


        // Shutdown the executor service
        executorService.shutdown();
    }

    // 3. Increment the same value with 120 threads, but create a new transaction 1_000_000 times for each increment
    // in the thread. Check the execution time of all transactions in comparison with the execution time in one thread.
//    @Test
//    public void modifyValueWith12Threads1_000_000NewTransactionsOnEachIncrementTest() throws ExecutionException, InterruptedException {
//        ExecutorService service = Executors.newCachedThreadPool();
//        Map<String, String> keyValTransactions = new HashMap<>();
//        var mvccComponent = new MVCCDataStructure();
//
//        String keyString = "key-" + (int) (Math.random() * 100000);
//        AtomicLong value = new AtomicLong(1000);
//        keyValTransactions.put(keyString, String.valueOf(value));
//
//        long startTime = System.nanoTime();
//
//        for (int i = 0; i < 12; i++) {
//            for (int j = 0; j < 10_000; j++) { // todo: replace with 1_000_000, for this first come up with the GC for OL (tested on mock
//                var th = service.submit(() -> {
//                    Transaction writeTransaction = mvccComponent.startWriteTransaction();
//                    // check record is null before the commit
//                    mvccComponent.put(writeTransaction, StringBinding.stringToEntry(keyString),
//                            StringBinding.stringToEntry(String.valueOf(value)));
//
//                    Assert.assertEquals(writeTransaction.getSnapshotId(), writeTransaction.getOperationLinkList().get(0).getTxId());
//                    try {
//                        mvccComponent.commitTransaction(writeTransaction);
//                    } catch (ExecutionException | InterruptedException e) {
//                        throw new ExodusException(e);
//                    }
//                });
//                th.get();
//            }
//            value.getAndIncrement();
//        }
//        System.out.println("Execution time in millis: " + (System.nanoTime() - startTime));
//        Assert.assertEquals(value.get(), 1012);
//    }

}
