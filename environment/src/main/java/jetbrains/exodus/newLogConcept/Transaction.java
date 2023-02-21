package jetbrains.exodus.newLogConcept;

import it.unimi.dsi.fastutil.longs.LongLongPair;

import java.util.ArrayList;

enum TransactionState {
    IN_PROGRESS,
    ABORTED,
    COMPLETED
}

public class Transaction {
    final TransactionType type;
    long snapshotId;
    OperationReferenceEntry operationLink; // array of links to record in OL
    ArrayList<LongLongPair> hashAddressPairList = new ArrayList<>();
    Transaction(long snapshotId,
                TransactionType type) {
        this.snapshotId = snapshotId;
        this.type = type;
    }

    void setOperationLink(OperationReferenceEntry linkEntry){
        this.operationLink = linkEntry;
    }

    public void addToHashAddressPairList(LongLongPair hashAddressPair) {
        this.hashAddressPairList.add(hashAddressPair);
    }

}