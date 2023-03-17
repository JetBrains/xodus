package jetbrains.exodus.entitystore;

import jetbrains.exodus.newLogConcept.GarbageCollector.MVCCGarbageCollector;
import jetbrains.exodus.newLogConcept.GarbageCollector.TransactionGCEntry;
import jetbrains.exodus.newLogConcept.MVCC.MVCCRecord;
import jetbrains.exodus.newLogConcept.Transaction.TransactionState;
import org.jctools.maps.NonBlockingHashMapLong;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class MVCCGarbageCollectorTest {

    @Test
    public void testFindMaxMinIdOneThread() {
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();

        for (long i = 1; i < 4; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }
        for (long i = 6; i < 7; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }

        for (long i = 7; i < 9; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        }

        var collector = new MVCCGarbageCollector();
        Assert.assertEquals(collector.findMaxMinId(transactionsGCMap, 10L).longValue(), 3L);

    }
}
