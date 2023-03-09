package jetbrains.exodus.entitystore;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.newLogConcept.Transaction;
import org.junit.Assert;
import org.junit.Test;
import jetbrains.exodus.newLogConcept.MVCCDataStructure;

import java.util.HashMap;
import java.util.Map;

import static jetbrains.exodus.entitystore.TestBase.logger;

public class MVCCComponentTest {


    // Get/put test
    // 1.1  make a loop in which the keys will be inserted and then read. And first one key, then 2,
    // then 4 up to 64 * 1024 in one transaction. So first interposed in one transaction, then read, then
    // committed. And read again. All keys are randomly generated.
    // 1.1.2 Start a separate thread before the commit and check that it does not see the changes before the commit,
    // and after the commit it sees all the changes.
    @Test
    // FIXME
    public void testReadCommitted() {

        int keyCounter = 1;
        Map<String, String> keyValTransactions = new HashMap<>();

        var mvccComponent = new MVCCDataStructure();
        while (keyCounter <=  64 * 1024) {
            int localKeyCounter = keyCounter;
            logger.info("Counter: " + keyCounter + " " + localKeyCounter);
            new Thread(() -> {
                var writeTransaction = mvccComponent.startWriteTransaction();
                logger.info("Put " + localKeyCounter + " key-value pairs");
                for (int i = 0; i < localKeyCounter; i++) {
                    putKeyAndCheckReadNull(keyValTransactions, mvccComponent, writeTransaction);
                }
                mvccComponent.commitTransaction(writeTransaction);
            }).start();
            checkReadRecordIsNotNull(keyValTransactions, mvccComponent);
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
    public void putDeleteInAnotherTransactionTest() {

        int keyCounter = 1;
        Map<String, String> keyValTransactionsPut = new HashMap<>();

        var mvccComponent = new MVCCDataStructure();
        while (keyCounter <=  64 * 1024) {
            int localKeyCounter = keyCounter;
            new Thread(() -> {
                var writeTransaction = mvccComponent.startWriteTransaction();
                logger.info("Put " + localKeyCounter + " key-value pairs");
                for (int i = 0; i < localKeyCounter; i++) {
                    putKeyAndCheckReadNull(keyValTransactionsPut, mvccComponent, writeTransaction);
                }
                mvccComponent.commitTransaction(writeTransaction);
            }).start();
            checkReadRecordIsNotNull(keyValTransactionsPut, mvccComponent);
            final Map<String, String> keyValTransactionsDelete = getSubmap(keyValTransactionsPut);

            new Thread(() -> {
                var writeTransaction = mvccComponent.startWriteTransaction();
                logger.info("Delete " + localKeyCounter / 2 + " key-value pairs");
                for (int i = 0; i < localKeyCounter / 2; i++) {
                    deleteKeyAndCheckReadNull(keyValTransactionsDelete, mvccComponent, writeTransaction);
                }
                mvccComponent.commitTransaction(writeTransaction);
            }).start();
            for (var keyValPair : keyValTransactionsDelete.entrySet()){
                checkReadRecordIsNull(mvccComponent, StringBinding.stringToEntry(keyValPair.getKey()));
            }

            keyCounter *= 2;
        }

    }

    private HashMap<String, String> getSubmap(Map<String, String> map ) {
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

    private void putKeyAndCheckReadNull(Map<String, String> keyValTransactions,
                                                MVCCDataStructure mvccComponent, Transaction writeTransaction) {
        String keyString = "key-" + (int) (Math.random() * 100000);
        String valueString = "value-" + (int) (Math.random() * 100000);
        keyValTransactions.put(keyString, valueString);

        ByteIterable key = StringBinding.stringToEntry(keyString);
        ByteIterable value = StringBinding.stringToEntry(valueString);

        logger.info("Put key, value: " + keyString + " " + valueString); // TODO find a way to put key-vals in this map, now not fully correct
        mvccComponent.put(writeTransaction, key, value);
        checkReadRecordIsNull(mvccComponent, key);

    }

    private void deleteKeyAndCheckReadNull(Map<String, String> keyValTransactions,
                                        MVCCDataStructure mvccComponent, Transaction writeTransaction) {
        for (var keyValPair : keyValTransactions.entrySet()) {
            var key = keyValPair.getKey();
            var value = keyValPair.getValue();
            logger.info("Remove key, value: " + keyValPair.getKey() + " " + keyValPair.getValue());
            mvccComponent.remove(writeTransaction, StringBinding.stringToEntry(key), StringBinding.stringToEntry(value));
            checkReadRecordIsNull(mvccComponent, StringBinding.stringToEntry(key));
        }

    }

    private void checkReadRecordIsNull(MVCCDataStructure mvccComponent, ByteIterable key) {
        new Thread(() -> {
            Transaction readTransaction = mvccComponent.startReadTransaction();
            ByteIterable record = mvccComponent.read(readTransaction, key);
            logger.info("Assert key " + key + " is null");
            Assert.assertNull(record);
        }).start();
    }

    private void checkReadRecordIsNotNull(Map<String, String> keyValTransactions,
                                          MVCCDataStructure mvccComponent) {
        new Thread(() -> {
            Transaction readTransaction = mvccComponent.startReadTransaction();
            for (var keyValPair : keyValTransactions.entrySet()) {
                ByteIterable record = mvccComponent.read(readTransaction,
                        StringBinding.stringToEntry(keyValPair.getKey()));
                logger.info("Assert key, value: " + keyValPair.getKey() +
                        " " + keyValPair.getValue());
                Assert.assertEquals(keyValPair.getValue(), StringBinding.entryToString(record));
            }
        }).start();
    }

    // 2.2 Add keys and delete in the same transaction. Check visibility before and after a commit in the current
    // thread and in a separate thread.
    @Test
    public void putDeleteInSameTransactionTest() {

    }
}
