package jetbrains.exodus.newLogConcept.GarbageCollector;

import jetbrains.exodus.newLogConcept.MVCC.MVCCRecord;
import jetbrains.exodus.newLogConcept.Transaction.TransactionState;
import org.jctools.maps.NonBlockingHashMapLong;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
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
                if (prevKey == null || currentKey == prevKey + 1) {
                    prevKey = currentKey;
                    maxMinId = currentKey;
                } else {
                    break;
                }
            }
        }
        return maxMinId;
    }


    void removeUpToMaxMinId(long snapshotId, NonBlockingHashMapLong<MVCCRecord> mvccHashMap,
                            ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap) {
        AtomicLong maxMinId = new AtomicLong(findMaxMinId(transactionsGCMap, snapshotId)); //todo define consistent type everywhere
        // todo check multithreading everywhere
        removeTransactionsRange(transactionsGCMap.firstKey(), maxMinId.getAndDecrement(), mvccHashMap,
                transactionsGCMap, true);
    }

    void removeTransactionsRange(long transactionIdStart, long transactionIdEnd,
                                 NonBlockingHashMapLong<MVCCRecord> mvccHashMap,
                                 ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap,
                                 boolean isUpToMaxMinId) {
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
