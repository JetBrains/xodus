package jetbrains.exodus.log;


import jetbrains.exodus.ByteIterable;

import java.util.concurrent.atomic.AtomicLong;


class OperationLogRecord {
    final ByteIterable key; // final thread-safety for fields, primitives for access speed
    // final makes no sense for methods and classes for the multi-threading
    final ByteIterable value;
    final AtomicLong transactionId;
    final int operationId; // shouldn't be strings, but leave like this for now

    OperationLogRecord(ByteIterable key, ByteIterable value, AtomicLong transactionId, int operationId) {
        this.key = key;
        this.value = value;
        this.transactionId = transactionId;
        this.operationId = operationId;
    }
}

