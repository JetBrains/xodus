package jetbrains.exodus.log;


import java.nio.ByteBuffer;

class OperationLogRecord {
    final ByteBuffer key; // final thread-safety for fields, primitives for access speed
    // final makes no sense for methods and classes for the multi-threading
    final ByteBuffer value;
    final long transactionId;
    final String operation; // shouldn't be strings, but leave like this for now

    OperationLogRecord(ByteBuffer key, ByteBuffer value, long transactionId, String operation) {
        this.key = key;
        this.value = value;
        this.transactionId = transactionId;
        this.operation = operation;
    }
}

