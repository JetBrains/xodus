package jetbrains.exodus.log;
import jetbrains.exodus.ExodusException;
import org.jctools.maps.NonBlockingHashMapLong;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import net.jpountz.xxhash.XXHash64;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static jetbrains.exodus.log.BufferedDataWriter.XX_HASH_FACTORY;
import static jetbrains.exodus.log.BufferedDataWriter.XX_HASH_SEED;

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
    final ArrayList<OperationReference> referencesToOperations;
    final long txId;

    OperationsLinksEntry(ArrayList<OperationReference> linksToOperations, long txId) {
        this.referencesToOperations = linksToOperations;
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
    public static final XXHash64 xxHash = XX_HASH_FACTORY.hash64();
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
    public static ByteBuffer readLogRecord(long currentTransactionId, ByteBuffer key) {

        final long keyHashCode = xxHash.hash(key, XX_HASH_SEED);

        MVCCRecord mvccRecord = hashMap.computeIfAbsent((long) keyHashCode, createRecord);
        compareWithCurrentAndSet(mvccRecord, currentTransactionId); //increment version

        // advanced approach: state-machine
        final AtomicLong minMaxValue = new AtomicLong(0);
        final AtomicReference<OperationsLinksEntry> targetEntry = new AtomicReference<>();

        var maxTxId = mvccRecord.maxTransactionId.get();
        mvccRecord.linksToOperations.forEach( linkEntry -> {
            var candidate = linkEntry.txId;
            var currentMax = minMaxValue.get();
            if (candidate < maxTxId && candidate > currentMax) {
                while (linkEntry.referencesToOperations.stream().anyMatch(it -> it.state == OperationReferenceState.IN_PROGRESS)){
                    Thread.onSpinWait(); // pass to the next thread, not to waste resources
                }
                if(linkEntry.referencesToOperations.stream().noneMatch(it -> it.state != OperationReferenceState.ABORTED)) {
                    minMaxValue.compareAndSet(currentMax, candidate);
                    targetEntry.set(linkEntry);
                }
            }
        });
        // todo not sure which approach is better - (q1)
//        var tmpEntrySet = operationLog.entrySet().stream()
//                .filter(it -> targetEntry.get().referencesToOperations.stream()
//                        .anyMatch(it2 -> it2.address == it.getKey()));
//        var targetOperationInLog = tmp.findFirst();

        OperationLogRecord targetOperationInLog = null;
        for (var entry: targetEntry.get().referencesToOperations) {
            targetOperationInLog = operationLog.get(entry.address);
            if (targetOperationInLog != null){
                break;
            }
        }

        if (targetOperationInLog == null){
            searchInBTree(key);
        }

        if (targetOperationInLog.key == key){
            return targetOperationInLog.value;
        } else {
            ArrayList<OperationsLinksEntry> selectionOfLessThanMaxTxId = new ArrayList<>();
            mvccRecord.linksToOperations.forEach( linkEntry -> {
                if (linkEntry.txId < maxTxId) {
                    while (linkEntry.referencesToOperations.stream().anyMatch(it -> it.state == OperationReferenceState.IN_PROGRESS)){
                        Thread.onSpinWait();
                    }
                    //todo - if all referencesToOperations.state != ABORTED or any? probably any? (q2)
                    if (linkEntry.referencesToOperations.state != OperationReferenceState.ABORTED) {
                        selectionOfLessThanMaxTxId.add(linkEntry);
                    }
                }
            });
            selectionOfLessThanMaxTxId.sort(Comparator.comparing(OperationsLinksEntry::getTxId).reversed());
            for (OperationsLinksEntry linkEntry: selectionOfLessThanMaxTxId) {
                targetOperationInLog = operationLog.get(linkEntry.referencesToOperations.address); // todo - do after (q1) solved
                if (targetOperationInLog.key == key){
                    return targetOperationInLog.value;
                }
            }


        }
        // potentially we can use ArrayList
        return searchInBTree(key);
    }

    private static ByteBuffer searchInBTree(ByteBuffer key){
        // mock method for the search of the operation in B-tree
        return ByteBuffer.allocate(0);
    }

    public static void write(long transactionId, ByteBuffer key, ByteBuffer value, String inputOperation) {
        final long keyHashCode = xxHash.hash(key, XX_HASH_SEED);
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











