package jetbrains.exodus.newLogConcept;
import it.unimi.dsi.fastutil.longs.LongLongImmutablePair;
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



enum OperationType {
    PUT, //todo convert to int later, for ex 0 and 1
    REMOVE
}

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
    private static final NonBlockingHashMapLong<TransactionState> transactionsStateMap =
            new NonBlockingHashMapLong<>(); // txID + state

    private static final AtomicLong address = new AtomicLong();
    private static AtomicLong snapshotId = new AtomicLong(1L); //todo initial value?
    private static AtomicLong writeTxSnapshotId = snapshotId;
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

    // should be separate for with duplicates and without, for now we do without only
    // todo in get() if we have remove, return NULL
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
                while (linkEntry.state == OperationReferenceState.IN_PROGRESS) {
                    Thread.onSpinWait(); // pass to the next thread, not to waste resources
                }
                if (linkEntry.state != OperationReferenceState.ABORTED) {
                    minMaxValue = candidateTxId;
                    targetEntry = linkEntry;
                }
            }
        }

        // case for REMOVE operation
        if (targetEntry == null){
            return null;
        }

        OperationLogRecord targetOperationInLog = operationLog.get(targetEntry.operationAddress);

        // case for error - smth goes wrong
        if (targetOperationInLog == null){
            throw new ExodusException();
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

    public void put(Transaction transaction,
                           ByteIterable key,
                           ByteIterable value) {
        addToLog(transaction, key, value, OperationType.PUT);
    }

    public void remove(Transaction transaction,
                              ByteIterable key,
                              ByteIterable value) {
        addToLog(transaction, key, value, OperationType.REMOVE);
    }

    public void addToLog(Transaction transaction,
                                ByteIterable key,
                                ByteIterable value,
                                OperationType operationType) {
        var recordAddress = address.getAndIncrement();
        final long keyHashCode = xxHash.hash(key.getBaseBytes(), key.baseOffset(), key.getLength(), XX_HASH_SEED);
        transaction.addToHashAddressPairList(new LongLongImmutablePair(keyHashCode, recordAddress));

        operationLog.put(recordAddress, new OperationLogRecord(key, value, snapshotId.get(), operationType));
        transaction.setOperationLink(new OperationReferenceEntry(recordAddress, snapshotId.get()));
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

    public void commitTransaction(Transaction transaction) {
        // todo replace to if (mvcc records collection is empty)
        if (transaction.type == TransactionType.WRITE){
            var currentSnapId = snapshotId;
            var keyHashCode = transaction.hashAddressPairList.first().firstLong(); // todo which one to take?
            MVCCRecord mvccRecord = hashMap.computeIfAbsent(keyHashCode, createRecord);
            hashMap.putIfAbsent(keyHashCode, mvccRecord);
            mvccRecord.linksToOperationsQueue.add(transaction.operationLink);

            // operation status check
            if (transaction.snapshotId < mvccRecord.maxTransactionId.get()) {
                transaction.operationLink.state = OperationReferenceState.ABORTED; // later in "read" we ignore this
                //pay att here - might require delete from mvccRecord.linksToOperationsQueue here
                throw new ExodusException(); // rollback
            }

            var txSnapId = transaction.snapshotId;
            if (currentSnapId.get() < txSnapId) {
                snapshotId.compareAndSet(currentSnapId.get(), txSnapId);
            }
            transaction.operationLink.state = OperationReferenceState.COMPLETED; // what we inserted "read" can see
            // advanced approach: state-machine
            //here we first work with collection, after that increment version, in read vica versa
        }
    }
}

// todo:  ROLLING_BACK (we put this state when read sees write which is in progress, not committed) for the transactions state
//        such transactions should be rolled back











