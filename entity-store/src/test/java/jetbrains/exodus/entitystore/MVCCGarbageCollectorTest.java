package jetbrains.exodus.entitystore;

import jetbrains.exodus.newLogConcept.GarbageCollector.MVCCGarbageCollector;
import jetbrains.exodus.newLogConcept.GarbageCollector.TransactionGCEntry;
import jetbrains.exodus.newLogConcept.MVCC.MVCCRecord;
import jetbrains.exodus.newLogConcept.Transaction.TransactionState;
import org.jctools.maps.NonBlockingHashMapLong;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class MVCCGarbageCollectorTest {

    @Test
    public void testFindMaxMinIdWithMissingIdsOneThread() {
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
        var collector = new MVCCGarbageCollector();
        Assert.assertEquals(collector.findMaxMinId(transactionsGCMap, 10L).longValue(), 3L);

    }

    @Test
    public void testFindMaxMinIdWithoutMissingIdsOneThread() {
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();

        for (long i = 1; i < 4; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }
        for (long i = 4; i < 6; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        }
        for (long i = 6; i < 7; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }
        var collector = new MVCCGarbageCollector();
        Assert.assertEquals(collector.findMaxMinId(transactionsGCMap, 10L).longValue(), 3L);
    }

    @Test
    public void testFindMaxMinIdWithRevertedIdsOneThread() {
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();

        for (long i = 1; i < 4; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }
        for (long i = 4; i < 6; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.REVERTED.get()));
        }
        for (long i = 6; i < 7; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }
        var collector = new MVCCGarbageCollector();
        Assert.assertEquals(collector.findMaxMinId(transactionsGCMap, 10L).longValue(), 6L);

    }
}
