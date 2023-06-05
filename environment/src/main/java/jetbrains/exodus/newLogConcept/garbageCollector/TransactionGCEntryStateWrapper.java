package jetbrains.exodus.newLogConcept.garbageCollector;

public class TransactionGCEntryStateWrapper {
    public volatile int state;
    public TransactionGCEntryStateWrapper(int state) {
        this.state = state;
    }
}
