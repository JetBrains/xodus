package jetbrains.exodus.newLogConcept;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.ExodusException;
import org.jctools.maps.NonBlockingHashMapLong;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import net.jpountz.xxhash.XXHash64;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
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
    //todo can add references to Transaction class properties via wrapper class?
    private static final NonBlockingHashMapLong<TransactionStateWrapper> transactionsStateMap =
            new NonBlockingHashMapLong<>(); // txID + state

    private static final AtomicLong address = new AtomicLong();
    private static final AtomicLong snapshotId = new AtomicLong(1L);
    private static final AtomicLong writeTxSnapshotId = snapshotId;
    private void compareWithCurrentAndSet(MVCCRecord mvccRecord, long currentTransactionId) {
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


    private ByteIterable searchInBTree(ByteIterable key){
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
        if (type == TransactionType.READ){
            newTransaction = new Transaction(snapshotId.get(), type);
        } else {
            newTransaction = new Transaction(writeTxSnapshotId.incrementAndGet(), type);
        }
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
        var snapshot = snapshotId.get();
        transaction.addOperationLink(new OperationReferenceEntry(recordAddress, snapshot, keyHashCode));
        operationLog.put(recordAddress, new TransactionOperationLogRecord(key, value, operationType));
    }


    // should be separate for with duplicates and without, for now we do without only
    // in get() if we have remove, return NULL
    public ByteIterable read(Transaction currentTransaction, ByteIterable key) {

        final long keyHashCode = xxHash.hash(key.getBaseBytes(), key.baseOffset(), key.getLength(), XX_HASH_SEED);

        MVCCRecord mvccRecord = hashMap.computeIfAbsent(keyHashCode, createRecord);
        compareWithCurrentAndSet(mvccRecord, currentTransaction.snapshotId); //increment version

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
                onProgressWait(currentTransaction);
                if (transactionsStateMap.get(currentTransaction.snapshotId).state != TransactionState.REVERTED) {
                    minMaxValue = candidateTxId;
                    targetEntry = linkEntry;
                }
            }
        }

        // case for REMOVE operation
        if (targetEntry == null){
            return null;
        }

        TransactionOperationLogRecord targetOperationInLog =
                (TransactionOperationLogRecord) operationLog.get(targetEntry.operationAddress);

        // case for error - smth goes wrong
        if (targetOperationInLog == null){
            throw new ExodusException();
        }

        if (targetOperationInLog.key.equals(key)){
            return targetOperationInLog.value;
        } else {

            ArrayList<OperationReferenceEntry> selectionOfLessThanMaxTxId = new ArrayList<>();
            mvccRecord.linksToOperationsQueue.forEach(linkEntry -> {
                waitAndAddLinkEntry(linkEntry, currentTransaction, selectionOfLessThanMaxTxId, maxTxId);
            });

            selectionOfLessThanMaxTxId.sort(Comparator.comparing(OperationReferenceEntry::getTxId).reversed());
            for (OperationReferenceEntry linkEntry: selectionOfLessThanMaxTxId) {
                targetOperationInLog = (TransactionOperationLogRecord)operationLog.get(linkEntry.operationAddress);
                if (targetOperationInLog.key.equals(key)){
                    return targetOperationInLog.value;
                }
            }
        }
        // potentially we can use ArrayList
        return searchInBTree(key);
    }

    private void waitAndAddLinkEntry(OperationReferenceEntry linkEntry,
                                     Transaction transaction,
                                     ArrayList<OperationReferenceEntry> selectionOfLessThanMaxTxId,
                                     long maxTxId) {
        if (linkEntry.txId < maxTxId) {
            onProgressWait(transaction);
            if (transactionsStateMap.get(transaction.snapshotId).state != TransactionState.REVERTED) {
                selectionOfLessThanMaxTxId.add(linkEntry);
            }
        }
    }

    void onProgressWait(Transaction transaction) {
        while (transactionsStateMap.get(transaction.snapshotId).state == TransactionState.IN_PROGRESS){
            CountDownLatch latch = transactionsStateMap.get(transaction.snapshotId).getLatchRef().get();
            if (latch != null){
                try {
                    latch.await();
                } catch (InterruptedException e){
                    throw new ExodusException();
                }
            } else {
                Thread.onSpinWait();// pass to the next thread, not to waste resources
            }
        }
    }

    public void commitTransaction(Transaction transaction) {
        // if transaction.type is WRITE
        if (!transaction.operationLinkList.isEmpty()){
            var currentSnapId = snapshotId;

            if (transaction.operationLinkList.size() > 10) {
                transactionsStateMap.get(transaction.snapshotId).initLatch();
            }

            for (var operation: transaction.operationLinkList) {
                MVCCRecord mvccRecord = mvccRecordCreateAndPut(operation);
                // operation status check
                if (transaction.snapshotId < mvccRecord.maxTransactionId.get()) {
                    transactionsStateMap.put(transaction.snapshotId,
                            new TransactionStateWrapper(TransactionState.REVERTED)); // later in "read" we ignore this
                    var recordAddress = address.getAndIncrement(); // put special record to log
                    operationLog.put(recordAddress, new TransactionCompletionLogRecord(true));

                    var latchRef = transactionsStateMap.get(transaction.snapshotId).getLatchRef();
                    CountDownLatch latch = latchRef.getAndSet(null);
                    if (latch != null) {
                        latch.countDown();
                    }

                    //pay att here - might require delete from mvccRecord.linksToOperationsQueue here
                    throw new ExodusException(); // rollback
                }
                while(true) {
                    var txSnapId = transaction.snapshotId;
                    if (currentSnapId.get() < txSnapId) {
                        snapshotId.compareAndSet(currentSnapId.get(), txSnapId);
                        break;
                    }
                }
                // advanced approach: state-machine
                //here we first work with collection, after that increment version, in read vica versa
            }
            transactionsStateMap.put(transaction.snapshotId, new TransactionStateWrapper(TransactionState.COMMITTED));

            var latchRef = transactionsStateMap.get(transaction.snapshotId).getLatchRef();
            CountDownLatch latch = latchRef.getAndSet(null);
            if (latch != null) {
                latch.countDown();
            }

            // what we inserted "read" can see
            var recordAddress = address.getAndIncrement(); // put special record to log
            operationLog.put(recordAddress, new TransactionCompletionLogRecord(false));
        }
    }

    private MVCCRecord mvccRecordCreateAndPut(OperationReferenceEntry operation){
        var keyHashCode = operation.keyHashCode;
        MVCCRecord mvccRecord = hashMap.computeIfAbsent(keyHashCode, createRecord);
        hashMap.putIfAbsent(keyHashCode, mvccRecord);
        mvccRecord.linksToOperationsQueue.add(operation);
        return mvccRecord;
    }
}

// todo:  ROLLING_BACK (we put this state when read sees write which is in progress, not committed) for the transactions state
//        such transactions should be rolled back











