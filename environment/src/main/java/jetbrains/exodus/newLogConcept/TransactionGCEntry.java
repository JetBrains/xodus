package jetbrains.exodus.newLogConcept;

public class TransactionGCEntry {

    TransactionStateWrapper stateWrapper;
    long upToId = -1;

    public TransactionGCEntry(int state) {
        this.stateWrapper = new TransactionStateWrapper(state);
    }
}

class TransactionGCEntryStateWrapper {
    volatile int state;
    public TransactionGCEntryStateWrapper(int state) {
        this.state = state;
    }
}


