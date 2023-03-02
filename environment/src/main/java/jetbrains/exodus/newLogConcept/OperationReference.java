package jetbrains.exodus.newLogConcept;


class OperationReferenceEntry {
    final long operationAddress; // not an array as we have multiple entries of OperationsLinksEntry in queue with same txId
    final long txId;
    final long keyHashCode;

    TransactionStateWrapper wrapper;

    OperationReferenceEntry(long linkToOperation, long txId, long keyHashCode) {
        this.operationAddress = linkToOperation;
        this.txId = txId;
        this.keyHashCode = keyHashCode;
    }

    public long getTxId() {
        return this.txId;
    }
}