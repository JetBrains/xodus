package jetbrains.exodus.newLogConcept;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


class TransactionStateWrapper {

    AtomicInteger state; // TODO: convert to volatile int

    private final AtomicReference<CountDownLatch> operationsCountLatchRef = new AtomicReference<>(); // TODO: convert to operationsCountLatchRef

    public int getState() {
        return state.get();
    }

    public void setState(int newState) {
        state.getAndSet(newState);
    }

    public TransactionStateWrapper(AtomicInteger state) {
        this.state = state;
    }

    void initLatch() {
        CountDownLatch newLatch = new CountDownLatch(1);
        if (operationsCountLatchRef.compareAndSet(null, newLatch)) {
            // the latch was set to a new object
        } else {
            // another thread already set the latch to a different object
            newLatch = null;
        }
    }
    public AtomicReference<CountDownLatch> getLatchRef() {
        return operationsCountLatchRef;
    }

}

