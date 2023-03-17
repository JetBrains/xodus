package jetbrains.exodus.newLogConcept.MVCC;


import jetbrains.exodus.newLogConcept.OperationLog.OperationReference;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class MVCCRecord {
    final AtomicLong maxTransactionId;

    // todo for later: optimization - create your own queue
    final ConcurrentLinkedQueue<OperationReference> linksToOperationsQueue; // temporary solution, will use faster queue

    public MVCCRecord(AtomicLong maxTransactionId, ConcurrentLinkedQueue<OperationReference> linksToOperations) {
        this.maxTransactionId = maxTransactionId;
        this.linksToOperationsQueue = linksToOperations;
    }
}