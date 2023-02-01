package jetbrains.exodus.log;
import org.jctools.maps.NonBlockingHashMapLong;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;


class OperationLogRecord {
    long key;
    long value;
    long transactionId;
    String operation; // todo shouldn't be strings

    OperationLogRecord(long key, long value, long transactionId, String operation) {
        this.key = key;
        this.value = value;
        this.transactionId = transactionId;
        this.operation = operation;
    }
}

class MVCCRecord {
    long maxTransactionId;
    SortedMap<Long, Long> linksToOperations; // txId + link to record in OL

    MVCCRecord(long maxTransactionId, SortedMap<Long, Long> linksToOperations) {
        this.maxTransactionId = maxTransactionId;
        this.linksToOperations = linksToOperations;
    }
}


class MVCCDataStructure {
    private static final NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); //primitive long keys
    private static final Map<Long, OperationLogRecord> operationLog = new ConcurrentSkipListMap<>();

    private static final AtomicLong address = new AtomicLong();


    // todo: separate for with duplicates and without, for now without

    public static long readLogRecord(long currentTransactionId, long key) {
        var keyHashCode = key.hashCode();
        // todo: put block not to add transactions less than ours
        // replace to compute (with lock) - get mvcc record and update (atomic long) maxtransactionID, compareAndSet
        // compute:
        MVCCRecord mvccRecord = hashMap.get(keyHashCode); //todo replace to compute (same as in write, for GC)
        // after compute execution we have mvccRecord in any case (instead of mvccRecord == null)
        // if the id of write transaction is less than ours, we prohibit to insert here
        // this inserted mvccRecord will be removed by GC
        // optimization: if during the execution of the compute we don't have any write transaction with id < ours, we don't need to insert
        // we need to make sure that we don't have such write transaction (or we have it)
//        if (mvccRecord == null) { // we don;t need it
//            return -1;
//        } else {
            if (mvccRecord.maxTransactionId < currentTransactionId) { // todo convert to cas (gonna be a cycle here)
                mvccRecord.maxTransactionId = currentTransactionId; // here we update and prohibit any write transaction to spol situation here
            }
//        }

        // find maximum id smaller than the id of the current transaction
        SortedMap<Long, Long> maxTxId;
        // todo: do binary search instead of cycle
        maxTxId = mvccRecord.linksToOperations.headMap(currentTransactionId);

        SortedMap<Long, Long> maxTxId2 =  maxTxId.reverse(); // from max to min
        // todo iterate operations and get log record, compare keys until they are matching, instead of
        //  OperationLogRecord logRecord = operationLog.get(mvccRecord.linksToOperations.get(maxTxId));


        // todo: key: byteArray (for later)
//        OperationLogRecord logRecord = operationLog.get(mvccRecord.linksToOperations.get(maxTxId));
//        if (logRecord.key.equals(key)){
//            return logRecord.value;
//        } else {
//            return -1;
//        }

    }

    public static void write(long transactionId, long key, long value) {
        long keyHashCode = key.hashCode();
        MVCCRecord mvccRecord = hashMap.get(keyHashCode);
        if (mvccRecord == null) {
            mvccRecord = new MVCCRecord(transactionId, new ConcurrentHashMap<>());
            hashMap.put(keyHashCode, mvccRecord);
        } // todo replace to compute

        if (transactionId < mvccRecord.maxTransactionId) {
            // do rollback (XodusException)
        }

        ArrayList<String> operations = new ArrayList<>();
        var addr = address.getAndIncrement();

        operationLog.put(addr, new OperationLogRecord(key, value, transactionId, operations));

        mvccRecord.linksToOperations.put(transactionId, addr);
    }
}















