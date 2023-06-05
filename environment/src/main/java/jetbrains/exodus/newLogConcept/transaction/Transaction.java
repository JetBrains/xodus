package jetbrains.exodus.newLogConcept.transaction;

import jetbrains.exodus.newLogConcept.operationLog.OperationReference;

import java.util.ArrayList;


public class Transaction {
    final TransactionType type;

    long snapshotId;

    ArrayList<OperationReference> operationLinkList = new ArrayList<>(); // array of links to record in OL
    public Transaction(long snapshotId, TransactionType type) {
        this.snapshotId = snapshotId;
        this.type = type;
    }

    public void addOperationReferenceEntryToList(OperationReference linkEntry){
        this.operationLinkList.add(linkEntry);
    }

    public long getSnapshotId() {
        return snapshotId;
    }


    public ArrayList<OperationReference> getOperationLinkList() {
        return operationLinkList;
    }


}

