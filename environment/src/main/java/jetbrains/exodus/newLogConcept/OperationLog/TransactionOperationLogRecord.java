package jetbrains.exodus.newLogConcept.OperationLog;

import jetbrains.exodus.ByteIterable;


public class TransactionOperationLogRecord implements OperationLogRecord {
    public final ByteIterable key; // final thread-safety for fields, primitives for access speed
    // final makes no sense for methods and classes for the multi-threading
    public final ByteIterable value;
    public final int operationType; // 0 - PUT, 1 - REMOVE

    public TransactionOperationLogRecord(ByteIterable key, ByteIterable value, int operationType) {
        this.key = key;
        this.value = value;
        this.operationType = operationType;
    }
}

