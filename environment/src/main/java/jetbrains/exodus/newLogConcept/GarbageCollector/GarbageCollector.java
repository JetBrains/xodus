package jetbrains.exodus.newLogConcept.GarbageCollector;

import jetbrains.exodus.newLogConcept.MVCC.MVCCRecord;
import org.jctools.maps.NonBlockingHashMapLong;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class GarbageCollector {

    AtomicLong findMaxMinId(long snapshotId) {
        // TODO not yet implemented
        return new AtomicLong(0L);
    }

    void removeUpToMaxMinId(long snapshotId, NonBlockingHashMapLong<MVCCRecord> mvccHashMap,
                            ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap) {
        AtomicLong maxMinId = findMaxMinId(snapshotId);
        // todo check multithreading everywhere
        removeTransactionsRange(transactionsGCMap.firstKey(), maxMinId.getAndDecrement(), mvccHashMap,
                transactionsGCMap, true);
    }

    void removeTransactionsRange(long transactionIdStart, long transactionIdEnd,
                                 NonBlockingHashMapLong<MVCCRecord> mvccHashMap,
                                 ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap,
                                 boolean isUpToMaxMinId) {
        for (long id = transactionIdEnd; id > transactionIdStart; id--){
            mvccHashMap.remove(id);
            transactionsGCMap.remove(id);
        }
        // if the sequence is up to maxMinId, then we delete the whole sequence, otherwise the first element is not deleted
        if (isUpToMaxMinId){
            mvccHashMap.remove(transactionIdStart);
            transactionsGCMap.remove(transactionIdStart);
        } else {
            transactionsGCMap.get(transactionIdStart).upToId = transactionIdEnd;
        }
    }
}
