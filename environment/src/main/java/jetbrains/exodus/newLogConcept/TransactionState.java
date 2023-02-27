package jetbrains.exodus.newLogConcept;

import java.util.concurrent.atomic.AtomicInteger;

public enum TransactionState {
    IN_PROGRESS(new AtomicInteger(1)),
    REVERTED(new AtomicInteger(2)),
    COMMITTED(new AtomicInteger(3));

    private final AtomicInteger intValue;

    TransactionState(AtomicInteger intValue) {
        this.intValue = intValue;
    }

    public int getInt() {
        return intValue.get();
    }
    public AtomicInteger getAtomic() {
        return intValue;
    }

}