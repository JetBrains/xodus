package jetbrains.exodus.newLogConcept;


import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

class MVCCRecord {
    final AtomicLong maxTransactionId;
    final ConcurrentLinkedQueue<OperationReferenceEntry> linksToOperationsQueue; // temporary solution, will use faster queue

    MVCCRecord(AtomicLong maxTransactionId, ConcurrentLinkedQueue<OperationReferenceEntry> linksToOperations) {
        this.maxTransactionId = maxTransactionId;
        this.linksToOperationsQueue = linksToOperations;
    }
}