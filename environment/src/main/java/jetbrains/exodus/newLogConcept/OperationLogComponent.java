package jetbrains.exodus.newLogConcept;


import jetbrains.exodus.ByteIterable;



class OperationLogRecord {
    final ByteIterable key; // final thread-safety for fields, primitives for access speed
    // final makes no sense for methods and classes for the multi-threading
    final ByteIterable value;
    final long transactionId; // todo do we actually need it
    final OperationType operationType;

    OperationLogRecord(ByteIterable key, ByteIterable value, long transactionId, OperationType operationType) {
        this.key = key;
        this.value = value;
        this.transactionId = transactionId;
        this.operationType = operationType;
    }
}

