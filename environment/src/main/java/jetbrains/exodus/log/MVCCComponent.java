package jetbrains.exodus.log;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import org.jctools.maps.NonBlockingHashMapLong;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import net.jpountz.xxhash.XXHash64;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static jetbrains.exodus.log.BufferedDataWriter.*;

class MVCCRecord {
    final AtomicLong maxTransactionId;
    final MpmcUnboundedXaddArrayQueue<OperationReferenceEntry> linksToOperationsQueue; // array of links to record in OL

    MVCCRecord(AtomicLong maxTransactionId, MpmcUnboundedXaddArrayQueue<OperationReferenceEntry> linksToOperations) {
        this.maxTransactionId = maxTransactionId;
        this.linksToOperationsQueue = linksToOperations;
    }
}

class MVCCDataStructure {
    public static final XXHash64 xxHash = XX_HASH_FACTORY.hash64();
    private static final NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys
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

    // should be separate for with duplicates and without, for now we do without only
    public static ByteIterable readLogRecord(long currentTransactionId, ByteIterable key) {

        final long keyHashCode = xxHash.hash(key.getBaseBytes(), key.baseOffset(), key.getLength(), XX_HASH_SEED);

        MVCCRecord mvccRecord = hashMap.computeIfAbsent(keyHashCode, createRecord);
        compareWithCurrentAndSet(mvccRecord, currentTransactionId); //increment version

        // advanced approach: state-machine
        long minMaxValue = 0;
        OperationReferenceEntry targetEntry = null;

        var maxTxId = mvccRecord.maxTransactionId.get();
        for (var linkEntry: mvccRecord.linksToOperationsQueue){
            var candidateTxId = linkEntry.txId;
            var currentMax = minMaxValue;

            // we add to queue several objects with one txID, the last one re-writes the previous value,
            // so we take the last with target txID
            if (candidateTxId < maxTxId && candidateTxId >= currentMax) {
                while (linkEntry.state == OperationReferenceState.IN_PROGRESS) {
                    Thread.onSpinWait(); // pass to the next thread, not to waste resources
                }
                if (linkEntry.state != OperationReferenceState.ABORTED) {
                    minMaxValue = candidateTxId;
                    targetEntry = linkEntry;
                }
            }
        }

        OperationLogRecord targetOperationInLog = operationLog.get(targetEntry.operationAddress); //todo null check

        if (targetOperationInLog == null){
            return searchInBTree(key);
        }

        if (targetOperationInLog.key.equals(key)){
            return targetOperationInLog.value;
        } else {
            ArrayList<OperationReferenceEntry> selectionOfLessThanMaxTxId = new ArrayList<>();
            mvccRecord.linksToOperationsQueue.forEach(linkEntry -> {
                if (linkEntry.txId < maxTxId) {
                    while (linkEntry.state == OperationReferenceState.IN_PROGRESS){
                        Thread.onSpinWait();
                    }
                    if (linkEntry.state != OperationReferenceState.ABORTED) {
                        selectionOfLessThanMaxTxId.add(linkEntry);
                    }
                }
            });
            selectionOfLessThanMaxTxId.sort(Comparator.comparing(OperationReferenceEntry::getTxId).reversed());
            for (OperationReferenceEntry linkEntry: selectionOfLessThanMaxTxId) {
                targetOperationInLog = operationLog.get(linkEntry.operationAddress);
                if (targetOperationInLog.key.equals(key)){
                    return targetOperationInLog.value;
                }
            }


        }
        // potentially we can use ArrayList
        return searchInBTree(key);
    }

    private static ByteIterable searchInBTree(ByteIterable key){
        // mock method for the search of the operation in B-tree
        return ByteIterable.EMPTY;
    }

    public static void write(long transactionId, ByteIterable key, ByteIterable value, String inputOperation) {
        final long keyHashCode = xxHash.hash(key.getBaseBytes(), key.baseOffset(), key.getLength(), XX_HASH_SEED);

        MVCCRecord mvccRecord = hashMap.computeIfAbsent(keyHashCode, createRecord);
        hashMap.putIfAbsent(keyHashCode, mvccRecord);

        var recordAddress = address.getAndIncrement();
        operationLog.put(recordAddress, new OperationLogRecord(key, value, transactionId, inputOperation));
        var linksEntry = new OperationReferenceEntry(recordAddress, transactionId);
        mvccRecord.linksToOperationsQueue.add(linksEntry);

        if (transactionId < mvccRecord.maxTransactionId.get()) {
            linksEntry.state = OperationReferenceState.ABORTED; // later in "read" we ignore this
            //pay att here - might require delete from mvccRecord.linksToOperationsQueue here
            throw new ExodusException(); // rollback
        }
        linksEntry.state = OperationReferenceState.COMPLETED; // what we inserted "read" can see
        // advanced approach: state-machine
        //here we first work with collection, after that increment version, in read vica versa
    }
}

// todo:  ROLLING_BACK (we put this state when read sees write which is in progress, not commited )for the transactions state
// todo:   such transactions should be rolledback











