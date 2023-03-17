package jetbrains.exodus.newLogConcept.OperationLog;

public class TransactionCompletionLogRecord implements OperationLogRecord {
    final boolean isRevertedFlag; // if 0 - commit, 1 - rollback

    public TransactionCompletionLogRecord(boolean isRollBackFlag) {
        this.isRevertedFlag = isRollBackFlag;
    }
}