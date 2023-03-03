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
        while (keyCounter < 1024) { // todo replace to 64 * 1024
            var writeTransaction = mvccComponent.startWriteTransaction();

            logger.debug("Put " + keyCounter + " key-value pairs");
            for (int i = 0; i < keyCounter; i++) {
                String keyString = "key-" + (int) (Math.random() * 100000);
                String valueString = "value-" + (int) (Math.random() * 100000);
                keyValTransactions.put(keyString, valueString);
                logger.debug("Put key, value: " + keyString + " " + valueString);

                ByteIterable key = StringBinding.stringToEntry(keyString);
                ByteIterable value = StringBinding.stringToEntry(valueString);
                mvccComponent.put(writeTransaction, key, value);

                new Thread(() -> {
                    Transaction readTransaction = mvccComponent.startReadTransaction();
                    ByteIterable record = mvccComponent.read(readTransaction, key);
                    Assert.assertNull(record);
                }).start();

            }

            mvccComponent.commitTransaction(writeTransaction);

            new Thread(() -> {
                Transaction readTransaction = mvccComponent.startReadTransaction();
                for (var keyValPair : keyValTransactions.entrySet()) {
                    ByteIterable record = mvccComponent.read(readTransaction,
                            StringBinding.stringToEntry(keyValPair.getKey()));
                    logger.debug("Assert key, value: " + keyValPair.getKey() +
                            " " + keyValPair.getValue());
                    Assert.assertEquals(keyValPair.getValue(), StringBinding.entryToString(record));
                }
            }).start();


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


    }

    // 2.2 Add keys and delete in the same transaction. Check visibility before and after a commit in the current
    // thread and in a separate thread.
    @Test
    public void putDeleteInSameTransactionTest() {

    }
}
