package jetbrains.exodus.newLogConcept;


public class OperationReference {
    final long operationAddress; // not an array as we have multiple entries of OperationsLinksEntry in queue with same txId
    final long txId;
    final long keyHashCode;

    TransactionStateWrapper wrapper;

    OperationReference(long linkToOperation, long txId, long keyHashCode) {
        this.operationAddress = linkToOperation;
        this.txId = txId;
        this.keyHashCode = keyHashCode;
    }

    public long getOperationAddress() {
        return operationAddress;
    }
    public long getTxId() {
        return this.txId;
    }
}