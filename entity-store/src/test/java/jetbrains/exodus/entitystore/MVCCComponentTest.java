package jetbrains.exodus.entitystore;

import org.junit.Test;

public class MVCCComponentTest {

    // One-thread tests

    // Get/put test
    // 1.1  make a loop in which the keys will be inserted and then read. And first one key, then 2,
    // then 4 up to 64 * 1024 in one transaction. So first interposed in one transaction, then read, then
    // committed. And read again. All keys are randomly generated.
    // Start a separate thread before the commit and check that it does not see the changes before the commit,
    // and after the commit it sees all the changes.
    @Test
    public void getPutKeysTest() {

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
