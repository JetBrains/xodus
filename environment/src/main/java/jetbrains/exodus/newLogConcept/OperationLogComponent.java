package jetbrains.exodus.newLogConcept;


import jetbrains.exodus.ByteIterable;


// todo later extends Loggable
interface OperationLogRecord { }


class TransactionOperationLogRecord implements OperationLogRecord {
    final ByteIterable key; // final thread-safety for fields, primitives for access speed
    // final makes no sense for methods and classes for the multi-threading
    final ByteIterable value;
    final int operationType; // 0 - PUT, 1 - REMOVE

    TransactionOperationLogRecord(ByteIterable key, ByteIterable value, int operationType) {
        this.key = key;
        this.value = value;
        this.operationType = operationType;
    }
}


class TransactionCompletionLogRecord implements OperationLogRecord {
    final boolean isRevertedFlag; // if 0 - commit, 1 - rollback

    TransactionCompletionLogRecord(boolean isRollBackFlag) {
        this.isRevertedFlag = isRollBackFlag;
    }
}