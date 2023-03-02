package jetbrains.exodus.newLogConcept;

public enum TransactionState {
    IN_PROGRESS(1),
    REVERTED(2),
    COMMITTED(3);

    private final int intValue;

    TransactionState(int intValue) {
        this.intValue = intValue;
    }

    public int get() {
        return intValue;
    }
}