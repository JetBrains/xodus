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
    LongLongPair hashAddressPair;

    // todo convert to array   ArrayList<LongLongPair> hashAddressPair = new ArrayList<>();
    Transaction(long snapshotId,
                TransactionType type) {
        this.snapshotId = snapshotId;
        this.type = type;
    }

    void setOperationLink(OperationReferenceEntry linkEntry){
        this.operationLink = linkEntry;
    }

    public void setHashAddressPair(LongLongPair hashAddressPair) {
        this.hashAddressPair = hashAddressPair;
    }

}