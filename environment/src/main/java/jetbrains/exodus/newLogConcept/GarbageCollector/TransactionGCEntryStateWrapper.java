package jetbrains.exodus.newLogConcept.GarbageCollector;

public class TransactionGCEntryStateWrapper {
    public volatile int state;
    public TransactionGCEntryStateWrapper(int state) {
        this.state = state;
    }
}
