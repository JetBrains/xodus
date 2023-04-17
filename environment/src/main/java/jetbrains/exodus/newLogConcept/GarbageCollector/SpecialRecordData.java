package jetbrains.exodus.newLogConcept.GarbageCollector;

import jetbrains.exodus.newLogConcept.Transaction.TransactionState;

public class SpecialRecordData {
    TransactionState state;
    long address;

    public SpecialRecordData(TransactionState state, long address) {
        this.state = state;
        this.address = address;
    }
}
