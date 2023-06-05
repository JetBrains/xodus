package jetbrains.exodus.newLogConcept.garbageCollector;

public class TransactionGCEntry {

    public TransactionGCEntryStateWrapper stateWrapper;

    long upToId = -1;

    public TransactionGCEntry(int state) {
        this.stateWrapper = new TransactionGCEntryStateWrapper(state);
    }

    public TransactionGCEntry(int state, long upTo) {
        this.stateWrapper = new TransactionGCEntryStateWrapper(state);
        this.upToId = upTo;
    }

}



