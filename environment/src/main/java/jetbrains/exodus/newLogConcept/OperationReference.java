package jetbrains.exodus.newLogConcept;

enum OperationReferenceState {
    IN_PROGRESS,
    ABORTED,
    COMPLETED
}

class OperationReferenceEntry {
    final long operationAddress; // not an array as we have multiple entries of OperationsLinksEntry in queue with same txId
    volatile OperationReferenceState state = OperationReferenceState.IN_PROGRESS;
    final long txId;
    final Long keyHashCode;


    OperationReferenceEntry(long linkToOperation, long txId, Long keyHashCode) {
        this.operationAddress = linkToOperation;
        this.txId = txId;
        this.keyHashCode = keyHashCode;
    }

    public long getTxId() {
        return this.txId;
    }
}