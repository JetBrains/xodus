package jetbrains.exodus.newLogConcept.GarbageCollector;

public class TransactionGCEntry {

    public TransactionGCEntryStateWrapper stateWrapper;

    long upToId = -1;

    public TransactionGCEntry(int state) {
        this.stateWrapper = new TransactionGCEntryStateWrapper(state);
    }

    public void setUpToId(long upToId) {
        this.upToId = upToId;
    }

}



