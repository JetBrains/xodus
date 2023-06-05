package jetbrains.exodus.newLogConcept.operationLog;

import jetbrains.exodus.ByteIterable;


public class TransactionOperationLogRecord implements OperationLogRecord {
    public final ByteIterable key; // final thread-safety for fields, primitives for access speed
    // final makes no sense for methods and classes for the multi-threading
    public final ByteIterable value;
    public final int operationType; // 0 - PUT, 1 - REMOVE
    final long transactionId;

    @Override
    public long getTransactionId() {
        return transactionId;
    }
    public TransactionOperationLogRecord(ByteIterable key, ByteIterable value, int operationType, long transactionId) {
        this.key = key;
        this.value = value;
        this.operationType = operationType;
        this.transactionId = transactionId;
    }

    @Override
    public LogRecordType getLogRecordType() {
        return LogRecordType.OPERATION;
    }
}

