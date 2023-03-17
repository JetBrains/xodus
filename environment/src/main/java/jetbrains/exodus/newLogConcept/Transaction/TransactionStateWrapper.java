package jetbrains.exodus.newLogConcept.Transaction;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


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

