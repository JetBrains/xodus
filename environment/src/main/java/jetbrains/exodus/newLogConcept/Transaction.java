package jetbrains.exodus.newLogConcept;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;


enum TransactionState {
    IN_PROGRESS(new AtomicInteger(1)),
    REVERTED(new AtomicInteger(2)),
    COMMITTED(new AtomicInteger(3));

    private final AtomicInteger intValue;

    TransactionState(AtomicInteger intValue) {
        this.intValue = intValue;
    }

    public int getInt() {
        return intValue.get();
    }
    public AtomicInteger getAtomic() {
        return intValue;
    }

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

