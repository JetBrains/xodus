package jetbrains.exodus.newLogConcept;

import java.util.ArrayList;


public class Transaction {
    final TransactionType type;
    long snapshotId;
    ArrayList<OperationReferenceEntry> operationLinkList = new ArrayList<>(); // array of links to record in OL
    Transaction(long snapshotId, TransactionType type) {
        this.snapshotId = snapshotId;
        this.type = type;
    }

    void addOperationReferenceEntryToList(OperationReferenceEntry linkEntry){
        this.operationLinkList.add(linkEntry);
    }


}

