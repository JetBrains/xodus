package jetbrains.exodus.newLogConcept;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


class TransactionStateWrapper {

    volatile int state;

    volatile CountDownLatch operationsCountLatchRef;

    public TransactionStateWrapper(int state) {
        this.state = state;
    }

    void initLatch(int size) {
        this.operationsCountLatchRef = new CountDownLatch(size);
    }

}

