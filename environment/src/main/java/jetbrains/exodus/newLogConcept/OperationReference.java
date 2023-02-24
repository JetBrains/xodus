package jetbrains.exodus.newLogConcept;



class OperationReferenceEntry {
    final long operationAddress; // not an array as we have multiple entries of OperationsLinksEntry in queue with same txId
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