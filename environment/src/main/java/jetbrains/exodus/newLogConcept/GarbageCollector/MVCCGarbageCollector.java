package jetbrains.exodus.newLogConcept.GarbageCollector;

import jetbrains.exodus.ExodusException;
import jetbrains.exodus.newLogConcept.MVCC.MVCCRecord;
import jetbrains.exodus.newLogConcept.Transaction.TransactionState;
import org.jctools.maps.NonBlockingHashMapLong;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

public class MVCCGarbageCollector {

    public Long findMaxMinId(ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap, Long snapshotId) {
        Long maxMinId = null;
        Long prevKey = null;

        for (Map.Entry<Long, TransactionGCEntry> entry
                : transactionsGCMap.headMap(snapshotId, true).entrySet()) {
            Long currentKey = entry.getKey();
            int state = entry.getValue().stateWrapper.state;
            if (state == TransactionState.COMMITTED.get() || state == TransactionState.REVERTED.get()) {
                if (prevKey == null || currentKey == prevKey + 1 || (currentKey - 1 <= transactionsGCMap.get(prevKey).upToId)) {
                    prevKey = currentKey;
                    maxMinId = currentKey;
                } else {
                    return getMaxFromRecord(transactionsGCMap, snapshotId, maxMinId);
                }
            } else {
                return getMaxFromRecord(transactionsGCMap, snapshotId, maxMinId);
            }
        }
        return getMaxFromRecord(transactionsGCMap, snapshotId, maxMinId);
    }

    private Long getMaxFromRecord(ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap,
                                  Long snapshotId, Long maxMinId){
        if (maxMinId == null)
            return null;
        var record = transactionsGCMap.get(maxMinId);
        if (record.upToId < snapshotId) {
            if (record.upToId != -1) {
                return record.upToId;
            } else {
                return maxMinId;
            }
        } else {
            return snapshotId - 1;
        }
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
        removeUpToMaxMinId(maxMinId, snapshotId, mvccHashMap, transactionsGCMap);

        ConcurrentSkipListSet<Long> activeOrEmptyTransactionsIds =
                findMissingOrActiveTransactionsIds(maxMinId, 15L, transactionsGCMap);
        removeBetweenActiveTransactions(maxMinId, activeOrEmptyTransactionsIds, snapshotId,
                mvccHashMap, transactionsGCMap);
    }

     //todo define consistent Long type everywhere
    private void removeUpToMaxMinId(Long maxMinId, long snapshotId, NonBlockingHashMapLong<MVCCRecord> mvccHashMap,
                            ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap) {
        // todo check multithreading everywhere
        if (snapshotId < maxMinId) {
            throw new ExodusException();
        }
        removeTransactionsRange(transactionsGCMap.firstKey(), maxMinId - 1, mvccHashMap,
                transactionsGCMap, true);
    }
    private void removeBetweenActiveTransactions(Long maxMinId, ConcurrentSkipListSet<Long> activeOrEmptyTransactionsIds,
                                                 long snapshotId, NonBlockingHashMapLong<MVCCRecord> mvccHashMap,
                                                 ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap) {

        // todo check multithreading everywhere
        if (snapshotId < maxMinId) {
            throw new ExodusException();
        }

        Iterator<Long> iterator = activeOrEmptyTransactionsIds.descendingIterator();
        Long prev = null;

        // prev and curr are active transactions, between them transactions are committed
        while (iterator.hasNext()) {
            Long curr = iterator.next();
            if (prev != null) {
                removeTransactionsRange(prev + 1, curr - 1, mvccHashMap,
                        transactionsGCMap, true);
            }
            prev = curr;
        }
    }

    void removeTransactionsRange(long transactionIdStart, long transactionIdEnd,
                                 NonBlockingHashMapLong<MVCCRecord> mvccHashMap,
                                 ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap,
                                 boolean isUpToMaxMinId) {
        if (transactionIdStart < transactionIdEnd){
            for (long id = transactionIdEnd; id > transactionIdStart; id--) {
                mvccHashMap.remove(id);
                transactionsGCMap.remove(id);
            }
            // if the sequence is up to maxMinId, then we delete the whole sequence, otherwise the first element is not deleted
            if (isUpToMaxMinId) {
                mvccHashMap.remove(transactionIdStart);
                transactionsGCMap.remove(transactionIdStart);
            } else {
                transactionsGCMap.get(transactionIdStart).upToId = transactionIdEnd;
            }
        }
    }
}
