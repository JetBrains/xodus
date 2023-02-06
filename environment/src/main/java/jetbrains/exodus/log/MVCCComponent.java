package jetbrains.exodus.log;
import jetbrains.exodus.ExodusException;
import org.jctools.maps.NonBlockingHashMapLong;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

class OperationReference {
    // for later: in case we have multiple operations we can use linked list as here we always have one thread per txId -> thread-safety !!
    final long address;
    volatile OperationReferenceState state = OperationReferenceState.IN_PROGRESS;

    OperationReference(long address, OperationReferenceState state) {
        this.address = address;
        this.state = state;
    }
}

enum OperationReferenceState {
    IN_PROGRESS,
    ABORTED,
    COMPLETED
}


class OperationsLinksEntry {
    final OperationReference linksToOperations;
    final long txId;

    OperationsLinksEntry(OperationReference linksToOperations, long txId) {
        this.linksToOperations = linksToOperations;
        this.txId = txId;
    }

    public long getTxId() {
        return this.txId;
    }
}

class MVCCRecord {
    final AtomicLong maxTransactionId;
    final MpmcUnboundedXaddArrayQueue<OperationsLinksEntry> linksToOperations; // array of links to record in OL

    MVCCRecord(AtomicLong maxTransactionId, MpmcUnboundedXaddArrayQueue<OperationsLinksEntry> linksToOperations) {
        this.maxTransactionId = maxTransactionId;
        this.linksToOperations = linksToOperations;
    }
}

class MVCCDataStructure {
    private static final NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); //primitive long keys
    private static final Map<Long, OperationLogRecord> operationLog = new ConcurrentSkipListMap<>();

    private static final AtomicLong address = new AtomicLong();

    private static void compareWithCurrentAndSet(MVCCRecord mvccRecord, long currentTransactionId) {
        while(true) {
            long value = mvccRecord.maxTransactionId.get();
            // prohibit any write transaction to spoil situation here
            if (value < currentTransactionId
                    && mvccRecord.maxTransactionId.compareAndSet(value, currentTransactionId)) {
                break;
            }
        }
    }

    // This one is static and non-lambdified to optimize performance
    private static final Function<Long, MVCCRecord> createRecord = new Function<>() {
        @Override
        public MVCCRecord apply(Long o) {
            return new MVCCRecord(new AtomicLong(0), new MpmcUnboundedXaddArrayQueue<>(16));
        }
    };

    // todo: WIP
    // should be separate for with duplicates and without, for now we do without only
    public static long readLogRecord(long currentTransactionId, long key) {

        var keyHashCode = Long.hashCode(key);
        MVCCRecord mvccRecord = hashMap.computeIfAbsent((long) keyHashCode, createRecord);
        compareWithCurrentAndSet(mvccRecord, currentTransactionId); //increment version

        // advanced approach: state-machine
        final AtomicLong minMaxValue = new AtomicLong(0);
        OperationsLinksEntry targetEntry;
        var maxTxId = mvccRecord.maxTransactionId.get();
        mvccRecord.linksToOperations.forEach( linkEntry -> {
            var candidate = linkEntry.txId;
            var currentMax = minMaxValue.get();
            if (candidate < maxTxId && candidate > currentMax) {
                while (linkEntry.linksToOperations.state == OperationReferenceState.IN_PROGRESS){
                    Thread.onSpinWait(); // pass to the next thread, not to waste resources
                }
                if(linkEntry.linksToOperations.state != OperationReferenceState.ABORTED) {
                    minMaxValue.compareAndSet(currentMax, candidate);
                    targetEntry = linkEntry;
                }
            }
        });
        var operation2 = operationLog.get(targetEntry.linksToOperations.address); // check if exists
        if (operation2.key == key){
            return operation2.value;
        } else {
            ArrayList<OperationsLinksEntry> selectionOfLessThanMaxTxId = new ArrayList<OperationsLinksEntry>();
            mvccRecord.linksToOperations.forEach( linkEntry -> {
                if (linkEntry.txId < maxTxId) {
                    while (linkEntry.linksToOperations.state == OperationReferenceState.IN_PROGRESS){
                        Thread.onSpinWait();
                    }
                    if (linkEntry.linksToOperations.state != OperationReferenceState.ABORTED) {
                        selectionOfLessThanMaxTxId.add(linkEntry);
                    }
                }
            });
            selectionOfLessThanMaxTxId.sort(Comparator.comparing(OperationsLinksEntry::getTxId).reversed());
            for (OperationsLinksEntry linkEntry: selectionOfLessThanMaxTxId) {
                operation2 = operationLog.get(linkEntry.linksToOperations.address);
                if (operation2.key == key){
                    return operation2.value;
                }
            }


        }
        // potentially we can use LinkedList
        return searchInBTree(key);
    }

    private static long searchInBTree(long key){
        // mock method for the search of the operation in B-tree
        return 0L;
    }

    public static void write(long transactionId, long key, long value, String inputOperation) {
        long keyHashCode = Long.hashCode(key);

        MVCCRecord mvccRecord = hashMap.computeIfAbsent(keyHashCode, createRecord);
        hashMap.putIfAbsent(keyHashCode, mvccRecord);

        var recordAddress = address.getAndIncrement();
        operationLog.put(recordAddress, new OperationLogRecord(key, value, transactionId, inputOperation));
        var operation = new OperationReference(recordAddress, OperationReferenceState.IN_PROGRESS);
        mvccRecord.linksToOperations.add(new OperationsLinksEntry(operation, transactionId));

        if (transactionId < mvccRecord.maxTransactionId.get()) {
            operation.state = OperationReferenceState.ABORTED; // later in "read" we ignore this
            //pay att here - might require delete from mvccRecord.linksToOperations here
            throw new ExodusException(); // rollback
        }
        operation.state = OperationReferenceState.COMPLETED; // what we inserted "read" can see
        // advanced approach: state-machine
        //here we first work with collection, after that increment version, in read vica versa
    }
}

// todo:  ROLLING_BACK (we put this state when read sees write which is in progress, not commited )for the transactions state
// todo:   such transactions should be rolledback











