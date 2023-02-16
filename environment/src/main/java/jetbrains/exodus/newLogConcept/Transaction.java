package jetbrains.exodus.newLogConcept;

import it.unimi.dsi.fastutil.longs.LongLongPair;

import java.util.ArrayList;

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
    long snapshotId;
    OperationReferenceEntry operationLink; // array of links to record in OL
    ArrayList<LongLongPair> hashAddressPair = new ArrayList<>();
    // todo add array with hashcode-address longLongPair (or smth similar)

    Transaction(long snapshotId,
                TransactionType type) {
        this.snapshotId = snapshotId;
        this.type = type;
    }

    void setOperationLink(OperationReferenceEntry linkEntry){
        this.operationLink = linkEntry;
    }
}