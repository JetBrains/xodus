package jetbrains.exodus.log;
import jetbrains.exodus.ExodusException;
import org.jctools.maps.NonBlockingHashMapLong;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;


class OperationLogRecord {
    final long key; // final thread-safety for fields, primitives for access speed
    // final makes no sense for methods and classes for the multi-threading
    final long value;
    final long transactionId;
    final String operation; // shouldn't be strings, but leave like this for now

    OperationLogRecord(long key, long value, long transactionId, String operation) {
        this.key = key;
        this.value = value;
        this.transactionId = transactionId;
        this.operation = operation;
    }
}

class OperationReference {
    // for later: in case we have multiple operations we can use linked list as here we always have one thread per txId -> thread-safety !!
    final long address;
    volatile State state = State.IN_PROGRESS;

    OperationReference(long address, State state) {
        this.address = address;
        this.state = state;
    }
}

class MVCCRecord {
    final AtomicLong maxTransactionId;
    final ConcurrentSkipListMap<Long, OperationReference> linksToOperations; // txId + link to record in OL

    MVCCRecord(AtomicLong maxTransactionId, ConcurrentSkipListMap<Long, OperationReference> linksToOperations) {
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

    // should be separate for with duplicates and without, for now we do without only
    public static long readLogRecord(long currentTransactionId, long key) {
        var keyHashCode = Long.hashCode(key);

        // todo: put block not to add transactions less than ours
        // replace to compute (with lock) - get mvcc record and update (atomic long) maxtransactionID, compareAndSet
        // todo: seems we don't need a custom compute here as autoboxing is not an issue here
        MVCCRecord mvccRecord = hashMap.computeIfAbsent((long) keyHashCode, (k) -> //todo redo lambda as static field not to re-create the object
            new MVCCRecord(new AtomicLong(0), new ConcurrentSkipListMap<>())
        );

        compareWithCurrentAndSet(mvccRecord, currentTransactionId); //increment version

        // advanced approach: state-machine

        // after compute execution we have mvccRecord in any case (instead of mvccRecord == null)
        // if the id of write transaction is less than ours, we prohibit to insert here
        // this inserted mvccRecord will be removed by GC
        // optimization: if during the execution of the compute we don't have any write transaction with id < ours,
        // we don't need to insert
        // we need to make sure that we don't have such write transaction (or we have it)


        // find maximum id smaller than the id of the current transaction


        // todo: use abstract class MpmcUnboundedXaddArrayQueue as linksToOperations
        //
        var maxTxId =  mvccRecord.linksToOperations.headMap(currentTransactionId);
        var reverseMaxTxId =  maxTxId.descendingMap(); // from max to min

        // Case:
        // readTxId=8, mvccRecord.linksToOperations.maxTxId=6 -> mvccRecord.maxTransactionId=8 (due to compareWithCurrentAndSet)
        // "write" tx inserts transaction with id=7,
        //         for (OperationReference operation : reverseMaxTxId.values())  read "6", so if "write" will insert 7,
        //         we won't see it as we started with 6 and below
        // in this case "write" will see that mvccRecord.maxTransactionId=8, and as it has 7, it ("write") will rollback itself
        // this way we avoid blocking

        // iterate operations and get log record, compare keys until they are matching

        for (element in q until reach currentTransactionId )
//        for (OperationReference operation : reverseMaxTxId.values()) {
            // if we don't see anything it means that "write" reverted itself, look at Case
            while (operation.state == State.IN_PROGRESS){
                Thread.onSpinWait(); // pass to the next thread, not to waste resources
            } // for "write" operation in progress, wait for it, for case when we see this "write" and wait till it finishes to understand if the element is valid
            if (operation.state == State.ABORTED) // "write" failed, not to ask mvccRecord.linksToOperations for the second time if it was deleted
                continue; // we go to the next record in for loop
        //----------------------------------
            var operation2 = operationLog.get(operation.address);
            if (operation2.key == key){
                return operation2.value;  // run cycle again for q
            }
        //----------------------------------
        }
        // potentially we can use LinkedList
        return searchInBTree(key);
    }

    long searchInBTree(long key){
        // mock method for the search of the operation in B-tree
        return 0L;
    }

    public static void write(long transactionId, long key, long value, String inputOperation) {
        long keyHashCode = Long.hashCode(key);
        MVCCRecord mvccRecord = hashMap.get(keyHashCode);
        if (mvccRecord == null) {
            mvccRecord = new MVCCRecord(transactionId, new SortedMap());
            hashMap.put(keyHashCode, mvccRecord);
        } // todo replace to compute

        var recordAddress = address.getAndIncrement();
        operationLog.put(recordAddress, new OperationLogRecord(key, value, transactionId, inputOperation));
        var operation = new OperationReference(recordAddress, State.IN_PROGRESS);
        mvccRecord.linksToOperations.put(transactionId, operation);

        if (transactionId < mvccRecord.maxTransactionId.get()) {
            operation.state = State.ABORTED; // later in "read" we ignore this
            hashMap.remove(keyHashCode);
            throw new ExodusException(); // rollback
        }
        operation.state = State.COMPLETED; // what we inserted "read" can see
        // advanced approach: state-machine

        //here we first work with collection, after that increment version, in read vica versa
    }


}

enum State {
    IN_PROGRESS,
    ABORTED,
    COMPLETED
}

// todo:  ROLLING_BACK (we put this state when read sees write which is in progress, not commited )for the transactions state
// todo:   such transactions should be rolledback











