package jetbrains.exodus.newLogConcept.GarbageCollector;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.newLogConcept.MVCC.MVCCRecord;
import jetbrains.exodus.newLogConcept.OperationLog.OperationLogRecord;
import jetbrains.exodus.newLogConcept.OperationLog.OperationReference;
import jetbrains.exodus.newLogConcept.OperationLog.TransactionOperationLogRecord;
import jetbrains.exodus.newLogConcept.Transaction.TransactionState;
import org.jctools.maps.NonBlockingHashMapLong;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class MVCCGarbageCollector {

    public Long findMaxMinId(ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap, Long snapshotId) {
        Long maxMinId = null;
        Long prevKey = null;

        for (Map.Entry<Long, TransactionGCEntry> entry
                : transactionsGCMap.headMap(snapshotId, false).entrySet()) {
            Long currentKey = entry.getKey();
            int state = entry.getValue().stateWrapper.state;
            if (state == TransactionState.COMMITTED.get() || state == TransactionState.REVERTED.get()) {
                if (prevKey == null || currentKey == prevKey + 1 || (currentKey - 1 <= transactionsGCMap.get(prevKey).upToId)) {
                    prevKey = currentKey;
                    maxMinId = currentKey;
                } else {
                    return maxMinId;
                }
            } else {
                return maxMinId;
            }
        }
        return maxMinId;
    }


    // todo multithreading
    public ConcurrentSkipListSet<Long> findMissingOrActiveTransactionsIds(Long maxMinId, Long snapshotId,
                                                                          ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap) {
        if (maxMinId != null && snapshotId < maxMinId) {
            throw new ExodusException();
        }
        ConcurrentSkipListSet<Long> result = new ConcurrentSkipListSet<>();
        Long lastKey = null;

        Long firstKey = transactionsGCMap.firstKey();
        if (maxMinId != null)
            firstKey = maxMinId + 1;

        for (long i = firstKey; i < snapshotId; i++) {
            if (isKeyMissing(i, transactionsGCMap)) {
                result.add(i);
            } else {
                if (transactionsGCMap.containsKey(i)) {
                    int state = transactionsGCMap.get(i).stateWrapper.state;
                    if (state == TransactionState.IN_PROGRESS.get() || (lastKey != null && i != lastKey + 1)) {
                        result.add(i);
                    }
                }
            }
            lastKey = i;
        }
        return result;
    }

    private boolean isKeyMissing(long key, ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap) {
        for (var element : transactionsGCMap.entrySet()) {
            if (element.getKey() == key || element.getValue().upToId >= key && element.getKey() < key) {
                return false;
            }
        }
        return true;
    }


    public void clean(long snapshotId, NonBlockingHashMapLong<MVCCRecord> mvccHashMap,
                      ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap) {

        Long maxMinId = findMaxMinId(transactionsGCMap, snapshotId);
        if (maxMinId != null)
            if (maxMinId >= snapshotId)
                throw new ExodusException();
        removeUpToMaxMinId(maxMinId, snapshotId, mvccHashMap, transactionsGCMap);

        ConcurrentSkipListSet<Long> activeOrEmptyTransactionsIds =
                findMissingOrActiveTransactionsIds(maxMinId, snapshotId, transactionsGCMap);

//        activeOrEmptyTransactionsIds.forEach(it ->  { assert it <= snapshotId; });
        removeBetweenActiveTransactions(maxMinId, activeOrEmptyTransactionsIds, snapshotId,
                mvccHashMap, transactionsGCMap);
    }

    private void removeUpToMaxMinId(Long maxMinId, long snapshotId, NonBlockingHashMapLong<MVCCRecord> mvccHashMap,
                                    ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap) {
        if (maxMinId == null) {
            return;
        }
        if (snapshotId < maxMinId) {
            throw new ExodusException();
        }
        removeTransactionsRange(transactionsGCMap.firstKey(), maxMinId - 1, mvccHashMap,
                transactionsGCMap, true);
    }

    private void removeBetweenActiveTransactions(Long maxMinId, ConcurrentSkipListSet<Long> activeOrEmptyTransactionsIds,
                                                 long snapshotId, NonBlockingHashMapLong<MVCCRecord> mvccHashMap,
                                                 ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap) {

        if (maxMinId == null) {
            return;
        }
        if (snapshotId < maxMinId) {
            throw new ExodusException();
        }

        Iterator<Long> iterator = activeOrEmptyTransactionsIds.iterator();
        Long prev = null;
        Long curr = -1L;
        // prev and curr are active transactions, between them transactions are committed
        while (iterator.hasNext()) {
            curr = iterator.next();
            if (prev != null) {
                removeTransactionsRange(prev + 1, curr - 1, mvccHashMap,
                        transactionsGCMap, false);
            }
            prev = curr;
        }
        removeTransactionsRange(curr + 1, maxMinId - 1, mvccHashMap,
                transactionsGCMap, true);
    }

    void removeTransactionsRange(long transactionIdStart, long transactionIdEnd,
                                 NonBlockingHashMapLong<MVCCRecord> mvccHashMap,
                                 ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap,
                                 boolean isUpToMaxMinId) {

        if (transactionIdStart < transactionIdEnd) {
            for (long id = transactionIdEnd; id > transactionIdStart; id--) {
                removeOperationsFromRecords(id, mvccHashMap);
                transactionsGCMap.remove(id);
            }
            // if the sequence is up to maxMinId, then we delete the whole sequence, otherwise the first element is not deleted
            if (isUpToMaxMinId) {
                removeOperationsFromRecords(transactionIdStart, mvccHashMap);
                transactionsGCMap.remove(transactionIdStart);
            } else {
                transactionsGCMap.get(transactionIdStart).upToId = transactionIdEnd;
            }
        }
    }

    private void removeOperationsFromRecords(long txId, NonBlockingHashMapLong<MVCCRecord> mvccHashMap) {
        for (var mvccRecord : mvccHashMap.entrySet()) {
            // remove operations with matching transactions ids from the queue
            for (OperationReference linkEntry : mvccRecord.getValue().linksToOperationsQueue) {
                if (linkEntry.getTxId() == txId) {
                    mvccRecord.getValue().linksToOperationsQueue.remove(linkEntry);
                }
            }
            if (mvccRecord.getValue().linksToOperationsQueue.isEmpty()) {
                mvccHashMap.computeIfPresent(mvccRecord.getKey(), (key, val) -> {
                    if (val.linksToOperationsQueue.isEmpty()) {
                        return null; // return null to delete the record
                    } else {
                        return val; // return the record itself if it should not be deleted
                    }
                });
            }
        }
    }

    public void handleMaxMinTransactionId(long snapshotId, NonBlockingHashMapLong<MVCCRecord> mvccHashMap,
                                   ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap,
                                   Map<Long, OperationLogRecord> operationLog) {
        Long maxMinId = findMaxMinId(transactionsGCMap, snapshotId);
        if (maxMinId != null)
            if (maxMinId >= snapshotId)
                throw new ExodusException();

        for (var record : mvccHashMap.entrySet()) {
            OperationReference targetEntry = null;

            for (OperationReference linkEntry : record.getValue().linksToOperationsQueue) {
                if (linkEntry.getTxId() == maxMinId) {
                    targetEntry = linkEntry;
                }
            }
            TransactionOperationLogRecord targetOperationInLog =
                    (TransactionOperationLogRecord) operationLog.get(targetEntry.getOperationAddress());

            // For maxMinSnapId in this record - if there is no link to it in OL, we can safely remove it
            if (targetOperationInLog == null) {
                record.getValue().linksToOperationsQueue.remove(targetEntry);
                transactionsGCMap.remove(maxMinId);
            }
            if (record.getValue().linksToOperationsQueue.isEmpty()) {
                mvccHashMap.computeIfPresent(record.getKey(), (key, val) -> {
                    if (val.linksToOperationsQueue.isEmpty()) {
                        return null; // return null to delete the record
                    } else {
                        return val;
                    }
                });
            }
        }
    }
}
