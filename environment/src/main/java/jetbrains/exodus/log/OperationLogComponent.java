package jetbrains.exodus.log;


import jetbrains.exodus.ByteIterable;


class OperationLogRecord {
    final ByteIterable key; // final thread-safety for fields, primitives for access speed
    // final makes no sense for methods and classes for the multi-threading
    final ByteIterable value;
    final long transactionId;
    final String operation; // shouldn't be strings, but leave like this for now

    OperationLogRecord(ByteIterable key, ByteIterable value, long transactionId, String operation) {
        this.key = key;
        this.value = value;
        this.transactionId = transactionId;
        this.operation = operation;
    }
}

