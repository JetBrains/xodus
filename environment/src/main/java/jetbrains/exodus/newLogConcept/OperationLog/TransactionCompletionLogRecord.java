package jetbrains.exodus.newLogConcept.OperationLog;

import jetbrains.exodus.newLogConcept.Transaction.TransactionState;

public class TransactionCompletionLogRecord implements OperationLogRecord {
    final boolean isRevertedFlag;
    final long transactionId;

    public TransactionCompletionLogRecord(boolean isRollBackFlag, long transactionId) {
        this.isRevertedFlag = isRollBackFlag;
        this.transactionId = transactionId;
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    public TransactionState getStatus() {
        if (isRevertedFlag){
            return TransactionState.REVERTED;
        } else {
            return TransactionState.COMMITTED;
        }
    }

    @Override
    public LogRecordType getLogRecordType() {
        return LogRecordType.COMPLETION;
    }
}