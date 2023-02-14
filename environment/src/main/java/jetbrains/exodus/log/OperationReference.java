package jetbrains.exodus.log;

import java.util.concurrent.atomic.AtomicLong;

enum OperationReferenceState {
    IN_PROGRESS,
    ABORTED,
    COMPLETED
}

class OperationReferenceEntry {
    final long operationAddress; // not an array as we have multiple entries of OperationsLinksEntry in queue with same txId
    volatile OperationReferenceState state = OperationReferenceState.IN_PROGRESS;
    final AtomicLong txId;


    OperationReferenceEntry(long linkToOperation, AtomicLong txId) {
        this.operationAddress = linkToOperation;
        this.txId = txId;
    }

    public AtomicLong getTxId() {
        return this.txId;
    }
}