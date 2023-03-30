package jetbrains.exodus.newLogConcept.MVCC;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import jetbrains.exodus.newLogConcept.GarbageCollector.MVCCGarbageCollector;
import jetbrains.exodus.newLogConcept.GarbageCollector.TransactionGCEntry;
import jetbrains.exodus.newLogConcept.OperationLog.OperationLogRecord;
import jetbrains.exodus.newLogConcept.OperationLog.OperationReference;
import jetbrains.exodus.newLogConcept.OperationLog.TransactionCompletionLogRecord;
import jetbrains.exodus.newLogConcept.OperationLog.TransactionOperationLogRecord;
import jetbrains.exodus.newLogConcept.Transaction.Transaction;
import jetbrains.exodus.newLogConcept.Transaction.TransactionState;
import jetbrains.exodus.newLogConcept.Transaction.TransactionStateWrapper;
import jetbrains.exodus.newLogConcept.Transaction.TransactionType;
import org.jctools.maps.NonBlockingHashMapLong;
import net.jpountz.xxhash.XXHash64;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static jetbrains.exodus.log.BufferedDataWriter.*;


public class MVCCDataStructure {
    public static final XXHash64 xxHash = XX_HASH_FACTORY.hash64();
    private final NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys
    private static final Map<Long, OperationLogRecord> operationLog = new ConcurrentSkipListMap<>();

    public final ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();

    private static final AtomicLong address = new AtomicLong();
    private final AtomicLong snapshotId = new AtomicLong(1L);
    private final AtomicLong writeTxSnapshotId = new AtomicLong(snapshotId.get()); // todo test

    // todo think later ab global mem usage
    private final long transactionsLimit = 1000; // the number of transactions, when reached we run GC

    private void compareWithCurrentAndSet(MVCCRecord mvccRecord, long currentTransactionId) {
        while (true) {
            long value = mvccRecord.maxTransactionId.get();
            // prohibit any write transaction to spoil situation here
            if (value < currentTransactionId) {
                if (mvccRecord.maxTransactionId.compareAndSet(value, currentTransactionId)) {
                    break;
                }
            } else {
                break;
            }
        }
    }


    // This one is static and non-lambdified to optimize performance
    private static final Function<Long, MVCCRecord> createRecord = new Function<>() {
        @Override
        public MVCCRecord apply(Long o) {
            return new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>());
        }
    };


    private ByteIterable searchInBTree(ByteIterable key) {
        // mock method for the search of the operation in B-tree
        return null;
    }

    // mock method for testing purposes
    void doSomething() {
        var transaction = startWriteTransaction();
        var key = ByteIterable.EMPTY;
        var value = ByteIterable.EMPTY;

        put(transaction, key, value);
        remove(transaction, key, value);

        commitTransaction(transaction);
    }


    public Transaction startReadTransaction() {
        return startTransaction(TransactionType.READ);
    }

    public Transaction startWriteTransaction() {
        return startTransaction(TransactionType.WRITE);
    }

    public Transaction startTransaction(TransactionType type) {
        Transaction newTransaction;
        if (type == TransactionType.READ) {
            newTransaction = new Transaction(snapshotId.get(), type);
        } else {
            newTransaction = new Transaction(writeTxSnapshotId.incrementAndGet(), type);
        }
        transactionsGCMap.put(newTransaction.getSnapshotId(), new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        return newTransaction;
    }

    public void put(Transaction transaction,
                    ByteIterable key,
                    ByteIterable value) {
        addToLog(transaction, key, value, 0);
    }

    public void remove(Transaction transaction,
                       ByteIterable key,
                       ByteIterable value) {
        addToLog(transaction, key, value, 1);
    }

    public void addToLog(Transaction transaction,
                         ByteIterable key,
                         ByteIterable value,
                         int operationType) {
        var recordAddress = address.getAndIncrement();
        final long keyHashCode = xxHash.hash(key.getBaseBytes(), key.baseOffset(), key.getLength(), XX_HASH_SEED);
        var snapshot = transaction.getSnapshotId();
        transaction.addOperationReferenceEntryToList(new OperationReference(recordAddress, snapshot, keyHashCode));
        operationLog.put(recordAddress, new TransactionOperationLogRecord(key, value, operationType));
    }


    // should be separate for with duplicates and without, for now we do without only
    // in get() if we have remove, return NULL
    public ByteIterable read(Transaction currentTransaction, ByteIterable key) {

        final long keyHashCode = xxHash.hash(key.getBaseBytes(), key.baseOffset(), key.getLength(), XX_HASH_SEED);

        MVCCRecord mvccRecord = hashMap.computeIfAbsent(keyHashCode, createRecord);
        compareWithCurrentAndSet(mvccRecord, currentTransaction.getSnapshotId()); //increment version

        // advanced approach: state-machine
        long maxMinValue = 0;
        OperationReference targetEntry = null;

        var maxTxId = mvccRecord.maxTransactionId.get();
        if (!mvccRecord.linksToOperationsQueue.isEmpty()) {
            for (OperationReference linkEntry : mvccRecord.linksToOperationsQueue) {
                var candidateTxId = linkEntry.getTxId();
                var currentMax = maxMinValue;
                // we add to queue several objects with one txID, the last one re-writes the previous value,
                // so we take the last with target txID
                if (candidateTxId <= maxTxId && candidateTxId >= currentMax) {
                    onProgressWait(linkEntry);
                    if (linkEntry.wrapper.state != TransactionState.REVERTED.get()) {
                        maxMinValue = candidateTxId;
                        targetEntry = linkEntry;
                    }
                }
            }
        }

        if (targetEntry == null) {
            return searchInBTree(key); // b-tree
        }

        TransactionOperationLogRecord targetOperationInLog =
                (TransactionOperationLogRecord) operationLog.get(targetEntry.getOperationAddress());

        // case for error - smth goes wrong
        if (targetOperationInLog == null) {
            throw new ExodusException();
        }

        // case for REMOVE operation
        if (targetOperationInLog.operationType == 1){
            return null;
        }

        if (targetOperationInLog.key.equals(key)) {
            return targetOperationInLog.value;
        } else {

            ArrayList<OperationReference> selectionOfLessThanMaxTxId = new ArrayList<>();
            mvccRecord.linksToOperationsQueue.forEach(linkEntry -> {
                waitAndAddLinkEntry(linkEntry, selectionOfLessThanMaxTxId, maxTxId);
            });

            selectionOfLessThanMaxTxId.sort(Comparator.comparing(OperationReference::getTxId).reversed());
            for (OperationReference linkEntry : selectionOfLessThanMaxTxId) {
                targetOperationInLog = (TransactionOperationLogRecord) operationLog.get(linkEntry.getOperationAddress());
                if (targetOperationInLog.key.equals(key)) {
                    return targetOperationInLog.value;
                }
            }
        }
        // potentially we can use ArrayList
        return searchInBTree(key);
    }

    private void waitAndAddLinkEntry(OperationReference linkEntry,
                                     ArrayList<OperationReference> selectionOfLessThanMaxTxId,
                                     long maxTxId) {
        if (linkEntry.getTxId() < maxTxId) {
            onProgressWait(linkEntry);
            if (linkEntry.wrapper.state != TransactionState.REVERTED.get()) {
                selectionOfLessThanMaxTxId.add(linkEntry);
            }
        }
    }

    void onProgressWait(OperationReference referenceEntry) {
        while (referenceEntry.wrapper.state == TransactionState.IN_PROGRESS.get()) {
            CountDownLatch latch = referenceEntry.wrapper.operationsCountLatchRef;
            if (latch != null) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new ExodusException(e);
                }
            } else {
                Thread.onSpinWait();// pass to the next thread, not to waste resources
            }
        }
    }

    public void commitTransaction(Transaction transaction) {
        // if transaction.type is WRITE
        if (!transaction.getOperationLinkList().isEmpty()) {
            // put state in PROGRESS
            var wrapper = new TransactionStateWrapper(TransactionState.IN_PROGRESS.get());

            if (transaction.getOperationLinkList().size() > 10) {
                wrapper.initLatch(1);
            }

            for (var operation : transaction.getOperationLinkList()) {
                operation.wrapper = wrapper;
                var transactionSnapId = transaction.getSnapshotId();
                MVCCRecord mvccRecord = mvccRecordCreateAndPut(operation, transactionSnapId);
                // operation status check
                if (transactionSnapId < mvccRecord.maxTransactionId.get()) {
                    wrapper.state = TransactionState.REVERTED.get();
                    transactionsGCMap.get(transactionSnapId).stateWrapper.state = TransactionState.REVERTED.get();
                    var recordAddress = address.getAndIncrement(); // put special record to log
                    operationLog.put(recordAddress, new TransactionCompletionLogRecord(true));

                    var latchRef = wrapper.operationsCountLatchRef;
                    if (latchRef != null) {
                        latchRef.countDown();
                        wrapper.operationsCountLatchRef = null;
                    }

                    //pay att here - might require delete from mvccRecord.linksToOperationsQueue here
                    throw new ExodusException(); // rollback
                }

                while (true) {
                    var txSnapId = transaction.getSnapshotId();
                    var currentSnapIdFixed = snapshotId.get();
                    if (currentSnapIdFixed < txSnapId) {
                        if (snapshotId.compareAndSet(currentSnapIdFixed, txSnapId)) {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                // advanced approach: state-machine
                //here we first work with collection, after that increment version, in read vica versa
            }
            wrapper.state = TransactionState.COMMITTED.get();
            transactionsGCMap.get(transaction.getSnapshotId()).stateWrapper.state = TransactionState.COMMITTED.get();

            var latchRef = wrapper.operationsCountLatchRef;
            if (latchRef != null) {
                latchRef.countDown();
                wrapper.operationsCountLatchRef = null;
            }

            // what we inserted "read" can see
            var recordAddress = address.getAndIncrement(); // put special record to log
            operationLog.put(recordAddress, new TransactionCompletionLogRecord(false));

            // run GC if needed, TODO fixme - see performance tests
//            if (transactionsGCMap.size() > transactionsLimit) {
//                ReentrantLock lock = new ReentrantLock();
//                if (lock.tryLock()){
//                    try {
//                        var collector = new MVCCGarbageCollector();
//                        collector.clean(snapshotId.get(), hashMap, transactionsGCMap);
//                    } finally {
//                        lock.unlock();
//                    }
//                }
//            }
        }
    }

    private MVCCRecord mvccRecordCreateAndPut(OperationReference operation, Long snapshotId) {
        var keyHashCode = operation.keyHashCode;
        MVCCRecord mvccRecord = hashMap.computeIfAbsent(keyHashCode, createRecord);
        compareWithCurrentAndSet(mvccRecord, snapshotId); //increment version

        hashMap.putIfAbsent(keyHashCode, mvccRecord);
        mvccRecord.linksToOperationsQueue.add(operation);
        return mvccRecord;
    }
}

// todo:  ROLLING_BACK (we put this state when read sees write which is in progress, not committed) for the transactions state
//        such transactions should be rolled back











