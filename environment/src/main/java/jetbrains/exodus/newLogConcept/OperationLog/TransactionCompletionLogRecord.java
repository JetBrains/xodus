package jetbrains.exodus.newLogConcept.OperationLog;

public class TransactionCompletionLogRecord implements OperationLogRecord {
    final boolean isRevertedFlag;
    final long transactionId;

    public TransactionCompletionLogRecord(boolean isRollBackFlag, long transactionId) {
        this.isRevertedFlag = isRollBackFlag;
        this.transactionId = transactionId;
    }

    @Override
    public LogRecordType getLogRecordType() {
        return LogRecordType.COMPLETION;
    }
}