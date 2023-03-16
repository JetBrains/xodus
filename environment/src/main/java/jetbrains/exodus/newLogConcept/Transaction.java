package jetbrains.exodus.newLogConcept;

import java.util.ArrayList;


public class Transaction {
    final TransactionType type;

    long snapshotId;

    ArrayList<OperationReference> operationLinkList = new ArrayList<>(); // array of links to record in OL
    Transaction(long snapshotId, TransactionType type) {
        this.snapshotId = snapshotId;
        this.type = type;
    }

    void addOperationReferenceEntryToList(OperationReference linkEntry){
        this.operationLinkList.add(linkEntry);
    }

    public long getSnapshotId() {
        return snapshotId;
    }


    public ArrayList<OperationReference> getOperationLinkList() {
        return operationLinkList;
    }


}

