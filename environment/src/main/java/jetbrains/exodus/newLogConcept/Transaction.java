package jetbrains.exodus.newLogConcept;

import java.util.ArrayList;

enum TransactionState {
    IN_PROGRESS,
    REVERTED,
    COMMITTED
}

public class Transaction {
    final TransactionType type;
    long snapshotId;
//    TransactionStateWrapper state; // todo replace to reference to object in hashmap
    ArrayList<OperationReferenceEntry> operationLinkList = new ArrayList<>(); // array of links to record in OL
    Transaction(long snapshotId, TransactionType type) {
        this.snapshotId = snapshotId;
        this.type = type;
    }

    void addOperationLink(OperationReferenceEntry linkEntry){
        this.operationLinkList.add(linkEntry);
    }


}

