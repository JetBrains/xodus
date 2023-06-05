package jetbrains.exodus.newLogConcept.operationLog;


import jetbrains.exodus.newLogConcept.transaction.TransactionStateWrapper;

public class OperationReference {
    final long operationAddress; // not an array as we have multiple entries of OperationsLinksEntry in queue with same txId
    final long txId;
    public final long keyHashCode;

    public TransactionStateWrapper wrapper;

    public OperationReference(long linkToOperation, long txId, long keyHashCode) {
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