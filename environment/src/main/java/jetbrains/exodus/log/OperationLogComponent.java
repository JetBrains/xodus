package jetbrains.exodus.log;



class OperationLogRecord {
    final long key; // final thread-safety for fields, primitives for access speed
    // final makes no sense for methods and classes for the multi-threading
    final long value;
    final long transactionId;
    final String operation; // shouldn't be strings, but leave like this for now

    OperationLogRecord(long key, long value, long transactionId, String operation) {
        this.key = key;
        this.value = value;
        this.transactionId = transactionId;
        this.operation = operation;
    }
}

