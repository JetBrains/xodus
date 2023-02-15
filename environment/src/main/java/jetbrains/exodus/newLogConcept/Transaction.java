package jetbrains.exodus.newLogConcept;


import java.util.concurrent.atomic.AtomicLong;

enum TransactionState {
    IN_PROGRESS,
    ABORTED,
    COMPLETED
}

enum TransactionType {
    READ,
    WRITE
}

class Transaction {
    final TransactionType type;
    final AtomicLong snapshotId;
    final OperationReferenceEntry operationLink; // array of links to record in OL

    Transaction(AtomicLong snapshotId,
                OperationReferenceEntry operationReference,
                TransactionType type) {
        this.snapshotId = snapshotId;
        this.operationLink = operationReference;
        this.type = type;
    }
}