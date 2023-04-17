package jetbrains.exodus.newLogConcept.OperationLog;

// todo later extends Loggable
public interface OperationLogRecord {


    long transactionId = 0;

    abstract long getTransactionId();

    abstract LogRecordType getLogRecordType();
}

