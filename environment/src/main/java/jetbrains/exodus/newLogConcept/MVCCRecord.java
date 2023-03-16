package jetbrains.exodus.newLogConcept;


import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

class MVCCRecord {
    final AtomicLong maxTransactionId;

    // todo for later: optimization - create your own queue
    final ConcurrentLinkedQueue<OperationReference> linksToOperationsQueue; // temporary solution, will use faster queue

    MVCCRecord(AtomicLong maxTransactionId, ConcurrentLinkedQueue<OperationReference> linksToOperations) {
        this.maxTransactionId = maxTransactionId;
        this.linksToOperationsQueue = linksToOperations;
    }
}