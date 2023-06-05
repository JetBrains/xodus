package jetbrains.exodus.newLogConcept.garbageCollector;

import jetbrains.exodus.newLogConcept.transaction.TransactionState;

public class SpecialRecordData {
    TransactionState state;
    long address;

    public SpecialRecordData(TransactionState state, long address) {
        this.state = state;
        this.address = address;
    }
}
