package jetbrains.exodus.newLogConcept;


import jetbrains.exodus.ByteIterable;


class OperationLogRecord {
    final ByteIterable key; // final thread-safety for fields, primitives for access speed
    // final makes no sense for methods and classes for the multi-threading
    final ByteIterable value;
    final int operationType; // 0 - PUT, 1 - REMOVE

    OperationLogRecord(ByteIterable key, ByteIterable value, int operationType) {
        this.key = key;
        this.value = value;
        this.operationType = operationType;
    }
    // todo add structure
}

