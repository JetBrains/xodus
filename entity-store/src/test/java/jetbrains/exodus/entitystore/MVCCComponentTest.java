package jetbrains.exodus.entitystore;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.newLogConcept.Transaction;
import org.junit.Assert;
import org.junit.Test;
import jetbrains.exodus.newLogConcept.MVCCDataStructure;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static jetbrains.exodus.entitystore.TestBase.logger;

public class MVCCComponentTest {


    // Get/put test
    // 1.1  make a loop in which the keys will be inserted and then read. And first one key, then 2,
    // then 4 up to 64 * 1024 in one transaction. So first interposed in one transaction, then read, then
    // committed. And read again. All keys are randomly generated.
    // 1.1.2 Start a separate thread before the commit and check that it does not see the changes before the commit,
    // and after the commit it sees all the changes.
    @Test
    public void testReadCommitted() throws ExecutionException, InterruptedException {

        int keyCounter = 1;
        ExecutorService service = Executors.newCachedThreadPool();

        while (keyCounter <= 64 * 1024) { //todo 64*1024
            logger.debug("Counter: " + keyCounter);
            Map<String, String> keyValTransactions = new HashMap<>();
            var mvccComponent = new MVCCDataStructure();

            for (int i = 0; i < keyCounter; i++) {
                String keyString = "key-" + (int) (Math.random() * 100000);
                String valueString = "value-" + (int) (Math.random() * 100000);
                keyValTransactions.put(keyString, valueString);
            }

            var th = service.submit(() -> {
                var writeTransaction = mvccComponent.startWriteTransaction();
                for (var entry : keyValTransactions.entrySet()) {
                    try {
                        // check record is null before the commit
                        putKeyAndCheckReadNull(entry, mvccComponent, writeTransaction, service);
                    } catch (ExecutionException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                mvccComponent.commitTransaction(writeTransaction);
                // check record is not null after the commit
                checkReadRecordIsNotNull(keyValTransactions, mvccComponent);
            });
            th.get();

            var th2 = service.submit(() -> {
                // check again that the record is not null after the commit after thread end
                checkReadRecordIsNotNull(keyValTransactions, mvccComponent);
            });
            th2.get();

            keyCounter *= 2;
        }
    }

    // 1.2 The same as 1.1, but not all keys are inserted in one transaction, but only 1/10th part.
    // Also check the visibility of each transaction in another thread.
    @Test
    public void getPutKeysPartlyTest() {

    }

    // Put/remove test
    // 2.1 Add keys in the same way (first 2, then 4, then 8). But then delete half in a separate transaction and
    // check for the presence of keys before and after the commit. Similarly, check the visibility of the transaction
    // in a separate thread.
    @Test
    public void putDeleteInAnotherTransactionTest() throws ExecutionException, InterruptedException {

        int keyCounter = 1;
        ExecutorService service = Executors.newCachedThreadPool();

        while (keyCounter <= 64 * 1024) {
            logger.debug("Counter: " + keyCounter);
            Map<String, String> keyValTransactions = new HashMap<>();
            var mvccComponent = new MVCCDataStructure();

            for (int i = 0; i < keyCounter; i++) {
                String keyString = "key-" + (int) (Math.random() * 100000);
                String valueString = "value-" + (int) (Math.random() * 100000);
                keyValTransactions.put(keyString, valueString);
            }

            //-----------------start PUT part check--------------------------------------

            var thPut1 = service.submit(() -> {
                var writeTransaction = mvccComponent.startWriteTransaction();
                for (var entry : keyValTransactions.entrySet()) {
                    try {
                        // check record is null before the commit
                        putKeyAndCheckReadNull(entry, mvccComponent, writeTransaction, service);
                    } catch (ExecutionException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                mvccComponent.commitTransaction(writeTransaction);
                // check record is not null after the commit
                checkReadRecordIsNotNull(keyValTransactions, mvccComponent);
            });
            thPut1.get();

            // check again that the record is not null after the commit after thread end
            var thPut2 = service.submit(() -> {
                checkReadRecordIsNotNull(keyValTransactions, mvccComponent);
            });
            thPut2.get();

            //-----------------end PUT part check--------------------------------------

            //-----------------start DELETE part check---------------------------------

            var thDelete1 = service.submit(() -> {
                var writeTransaction = mvccComponent.startWriteTransaction();
                try {
                    // check record is not null before the commit
                    deleteKeyAndCheckReadNotNull(keyValTransactions, mvccComponent, writeTransaction);
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                mvccComponent.commitTransaction(writeTransaction);
                // check record is null after the commit
                try {
                    for (var entry : keyValTransactions.entrySet()) {
                        checkReadRecordIsNull(mvccComponent, StringBinding.stringToEntry(entry.getKey()), service);
                    }
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            thDelete1.get();

            // check again that the record is not null after the commit after thread end
            var thDelete2 = service.submit(() -> {
                try {
                    for (var entry : keyValTransactions.entrySet()) {
                        checkReadRecordIsNull(mvccComponent, StringBinding.stringToEntry(entry.getKey()), service);
                    }
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            thDelete2.get();
            //-----------------end DELETE part check--------------------------------------
            keyCounter *= 2;
        }

    }


    // 2.2 Add keys and delete in the same transaction. Check visibility before and after a commit in the current
    // thread and in a separate thread.
    @Test
    public void putDeleteInSameTransactionTest() throws ExecutionException, InterruptedException {

        int keyCounter = 1;
        ExecutorService service = Executors.newCachedThreadPool();

        while (keyCounter <= 64 * 1024) {
            logger.debug("Counter: " + keyCounter);
            Map<String, String> keyValTransactions = new HashMap<>();
            var mvccComponent = new MVCCDataStructure();

            for (int i = 0; i < keyCounter; i++) {
                String keyString = "key-" + (int) (Math.random() * 100000);
                String valueString = "value-" + (int) (Math.random() * 100000);
                keyValTransactions.put(keyString, valueString);
            }

            var th = service.submit(() -> {
                var writeTransaction = mvccComponent.startWriteTransaction();
                //-----------------start PUT part check------------------------------------

                for (var entry : keyValTransactions.entrySet()) {
                    try {
                        // check record is null before the commit
                        putKeyAndCheckReadNull(entry, mvccComponent, writeTransaction, service);
                    } catch (ExecutionException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                //-----------------end PUT part check--------------------------------------

                //-----------------start DELETE part check---------------------------------

                try {
                    // check record is null before the commit
                    deleteKeyAndCheckReadNull(keyValTransactions, mvccComponent, writeTransaction, service);
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                mvccComponent.commitTransaction(writeTransaction);

                try {
                    // check record is null after the commit
                    for (var entry : keyValTransactions.entrySet()) {
                        checkReadRecordIsNull(mvccComponent, StringBinding.stringToEntry(entry.getKey()), service);
                    }
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            th.get();

            var th2 = service.submit(() -> {
                try {
                    // check that the record is null after the commit again after the thread end
                    for (var entry : keyValTransactions.entrySet()) {
                        checkReadRecordIsNull(mvccComponent, StringBinding.stringToEntry(entry.getKey()), service);
                    }
                } catch (ExecutionException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            th2.get();
            //-----------------end DELETE part check--------------------------------------

            keyCounter *= 2;
        }
    }


    private HashMap<String, String> getSubmap(Map<String, String> map) {
        HashMap<String, String> submap = new HashMap<>();
        final int limit = map.size() / 2;
        for (var keyValPair : map.entrySet()) {
            submap.put(keyValPair.getKey(), keyValPair.getValue());
            if (submap.size() == limit) {
                break;
            }
        }
        return submap;
    }

    private void putKeyAndCheckReadNull(Map.Entry<String, String> entry, MVCCDataStructure mvccComponent,
                                        Transaction writeTransaction, ExecutorService service) throws ExecutionException, InterruptedException {

        ByteIterable key = StringBinding.stringToEntry(entry.getKey());
        ByteIterable value = StringBinding.stringToEntry(entry.getValue());

        logger.debug("Put key, value: " + entry.getKey() + " " + entry.getValue() + " to mvcc");
        mvccComponent.put(writeTransaction, key, value);
        checkReadRecordIsNull(mvccComponent, key, service);

    }

    private void deleteKey(Map.Entry<String, String> keyValuePair, MVCCDataStructure mvccComponent,
                           Transaction writeTransaction) {
        var key = keyValuePair.getKey();
        var value = keyValuePair.getValue();
        logger.debug("Remove key, value: " + keyValuePair.getKey() + " " + keyValuePair.getValue());
        mvccComponent.remove(writeTransaction, StringBinding.stringToEntry(key), StringBinding.stringToEntry(value));
    }

    private void deleteKeyAndCheckReadNull(Map<String, String> keyValTransactions, MVCCDataStructure mvccComponent,
                                           Transaction writeTransaction, ExecutorService service) throws ExecutionException, InterruptedException {
        for (var keyValPair : keyValTransactions.entrySet()) {
            deleteKey(keyValPair, mvccComponent, writeTransaction);
            checkReadRecordIsNull(mvccComponent, StringBinding.stringToEntry(keyValPair.getKey()), service);
        }
    }

    private void deleteKeyAndCheckReadNotNull(Map<String, String> keyValTransactions, MVCCDataStructure mvccComponent,
                                              Transaction writeTransaction) throws ExecutionException, InterruptedException {
        for (var keyValPair : keyValTransactions.entrySet()) {
            deleteKey(keyValPair, mvccComponent, writeTransaction);
        }
        checkReadRecordIsNotNull(keyValTransactions, mvccComponent);
    }

    private void checkReadRecordIsNull(MVCCDataStructure mvccComponent, ByteIterable key,
                                       ExecutorService service) throws ExecutionException, InterruptedException {
        var th = service.submit(() -> {
            Transaction readTransaction = mvccComponent.startReadTransaction();
            ByteIterable record = mvccComponent.read(readTransaction, key);
            logger.debug("Assert key " + key + " is null");
            Assert.assertNull(record);
        });
        th.get();
    }

    private void checkReadRecordIsNotNull(Map<String, String> keyValTransactions,
                                          MVCCDataStructure mvccComponent) {
        Transaction readTransaction = mvccComponent.startReadTransaction();
        for (var keyValPair : keyValTransactions.entrySet()) {
            ByteIterable record = mvccComponent.read(readTransaction,
                    StringBinding.stringToEntry(keyValPair.getKey()));
            logger.debug("Assert key, value: " + keyValPair.getKey() +
                    " " + keyValPair.getValue());
            Assert.assertEquals(keyValPair.getValue(), StringBinding.entryToString(record));
        }

    }
}
