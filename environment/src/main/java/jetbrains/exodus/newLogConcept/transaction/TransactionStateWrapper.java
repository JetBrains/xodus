package jetbrains.exodus.newLogConcept.transaction;

import java.util.concurrent.CountDownLatch;


public class TransactionStateWrapper {

    public volatile int state;

    public volatile CountDownLatch operationsCountLatchRef;

    public TransactionStateWrapper(int state) {
        this.state = state;
    }

    public void initLatch(int size) {
        this.operationsCountLatchRef = new CountDownLatch(size);
    }

}

