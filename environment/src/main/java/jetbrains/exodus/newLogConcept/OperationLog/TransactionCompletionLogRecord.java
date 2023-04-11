package jetbrains.exodus.newLogConcept.OperationLog;

public class TransactionCompletionLogRecord implements OperationLogRecord {
    final boolean isRevertedFlag;

    public TransactionCompletionLogRecord(boolean isRollBackFlag) {
        this.isRevertedFlag = isRollBackFlag;
    }

    @Override
    public LogRecordType getLogRecordType() {
        return LogRecordType.COMPLETION;
    }
}