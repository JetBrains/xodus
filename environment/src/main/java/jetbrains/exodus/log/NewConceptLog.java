package jetbrains.exodus.log;
import jetbrains.exodus.ExodusException;
import org.jctools.maps.NonBlockingHashMapLong;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;


class OperationLogRecord {
    long key;
    long value;
    long transactionId;
    String operation; // shouldn't be strings, but leave like this for now

    OperationLogRecord(long key, long value, long transactionId, String operation) {
        this.key = key;
        this.value = value;
        this.transactionId = transactionId;
        this.operation = operation;
    }
}

class MVCCRecord {
    AtomicLong maxTransactionId;
    SortedMap<Long, Long> linksToOperations; // txId + link to record in OL

    MVCCRecord(AtomicLong maxTransactionId, SortedMap<Long, Long> linksToOperations) {
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
        MVCCRecord mvccRecord = hashMap.compute(keyHashCode, (k, v) -> {
            if (hashMap.get(keyHashCode) == null){
                hashMap.put(keyHashCode, new MVCCRecord(0, new SortedMap<Long, Long>()));
            }
            return hashMap.get(keyHashCode);
        });

        // after compute execution we have mvccRecord in any case (instead of mvccRecord == null)
        // if the id of write transaction is less than ours, we prohibit to insert here
        // this inserted mvccRecord will be removed by GC
        // optimization: if during the execution of the compute we don't have any write transaction with id < ours,
        // we don't need to insert
        // we need to make sure that we don't have such write transaction (or we have it)

        compareWithCurrentAndSet(mvccRecord, currentTransactionId);

        // find maximum id smaller than the id of the current transaction
        SortedMap<Long, Long> maxTxId;
        maxTxId = mvccRecord.linksToOperations.headMap(currentTransactionId);
        SortedMap<Long, Long> reverseMaxTxId =  maxTxId.reverse(); // from max to min
//        TreeMap<Long, Long> reverseTreeMap = new TreeMap(maxTxId).descendingMap();

        // iterate operations and get log record, compare keys until they are matching
        for (Long operationKey : operationLog.keySet()) {
            // todo: do binary search instead of cycle, use reverseMaxTxId and thread-safe comparison
            if (operationLog.get(operationKey).key == key){
                return operationLog.get(operationKey).value;
            }
        }
        return -1 ;// todo: what to do if no match - put in cycle
    }

    public static void write(long transactionId, long key, long value, String operation) {
        long keyHashCode = Long.hashCode(key);
        MVCCRecord mvccRecord = hashMap.get(keyHashCode);
        if (mvccRecord == null) {
            mvccRecord = new MVCCRecord(transactionId, new SortedMap());
            hashMap.put(keyHashCode, mvccRecord);
        } // todo replace to compute

        if (transactionId < mvccRecord.maxTransactionId.get()) {
            throw new ExodusException(); // rollback
        }

        var recordAddress = address.getAndIncrement();
        operationLog.put(recordAddress, new OperationLogRecord(key, value, transactionId, operation));
        mvccRecord.linksToOperations.put(transactionId, recordAddress);
    }
}















