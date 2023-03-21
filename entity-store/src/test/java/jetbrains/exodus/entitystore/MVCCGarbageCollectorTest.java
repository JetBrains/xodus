package jetbrains.exodus.entitystore;

import jetbrains.exodus.newLogConcept.GarbageCollector.MVCCGarbageCollector;
import jetbrains.exodus.newLogConcept.GarbageCollector.TransactionGCEntry;
import jetbrains.exodus.newLogConcept.MVCC.MVCCRecord;
import jetbrains.exodus.newLogConcept.Transaction.TransactionState;
import org.jctools.maps.NonBlockingHashMapLong;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
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

    // todo fixme
    @Test
    public void testFindMaxMinIdWithMergedIdsOneThread() {
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();

        for (long i = 1; i < 4; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            TransactionGCEntry entry = new TransactionGCEntry(TransactionState.COMMITTED.get());
            entry.setUpToId(6);
            transactionsGCMap.put(i, entry);
        }

        for (long i = 7; i < 8; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }
        var collector = new MVCCGarbageCollector();
        Assert.assertEquals(collector.findMaxMinId(transactionsGCMap, 10L).longValue(), 7L);

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


    @Test
    public void getActiveTransactionsIdsOneThread() {
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();

        for (long i = 1; i < 4; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }
        for (long i = 4; i < 7; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        }

        for (long i = 7; i < 10; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }

        var collector = new MVCCGarbageCollector();
        Long maxMinId = collector.findMaxMinId(transactionsGCMap, 15L);
        ConcurrentSkipListSet<Long> activeOrEmptyTransactionsIds =
                collector.findMissingOrActiveTransactionsIds(maxMinId, 15L, transactionsGCMap);

        ConcurrentSkipListSet<Long> correctActiveOrEmptyTransactionsIds =
                new ConcurrentSkipListSet<>(Arrays.asList(4L, 5L, 6L, 10L, 11L, 12L, 13L, 14L));

        Assert.assertEquals(correctActiveOrEmptyTransactionsIds, activeOrEmptyTransactionsIds);
    }

    @Test
    public void getMissingIdsOneThread() {
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();

        for (long i = 1; i < 4; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }
        for (long i = 5; i < 6; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.REVERTED.get()));
        }

        for (long i = 10; i < 11; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }

        var collector = new MVCCGarbageCollector();
        Long maxMinId = collector.findMaxMinId(transactionsGCMap, 11L);
        ConcurrentSkipListSet<Long> activeOrEmptyTransactionsIds =
                collector.findMissingOrActiveTransactionsIds(maxMinId, 11L, transactionsGCMap);

        ConcurrentSkipListSet<Long> correctActiveOrMissingTransactionsIds =
                new ConcurrentSkipListSet<>(Arrays.asList(4L, 6L, 7L, 8L, 9L));

        Assert.assertEquals(correctActiveOrMissingTransactionsIds, activeOrEmptyTransactionsIds);
    }

    @Test
    public void getActiveOMissingTransactionsIdsOneThread() {
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();

        for (long i = 1; i < 4; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }
        for (long i = 5; i < 6; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.REVERTED.get()));
        }
        for (long i = 6; i < 8; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        }
        for (long i = 9; i < 11; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }

        for (long i = 13; i < 20; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        }

        var collector = new MVCCGarbageCollector();
        Long maxMinId = collector.findMaxMinId(transactionsGCMap, 15L);
        ConcurrentSkipListSet<Long> activeOrEmptyTransactionsIds =
                collector.findMissingOrActiveTransactionsIds(maxMinId, 15L, transactionsGCMap);

        ConcurrentSkipListSet<Long> correctActiveOrEmptyTransactionsIds =
                new ConcurrentSkipListSet<>(Arrays.asList(4L, 6L, 7L, 8L, 11L, 12L, 13L, 14L));

        Assert.assertEquals(correctActiveOrEmptyTransactionsIds, activeOrEmptyTransactionsIds);
    }

    @Test
    public void getActiveAndMissingTransactionsIdsWithUpToOneThread() {
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();

        for (long i = 1; i < 3; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }

        transactionsGCMap.put(3L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 6L));
        hashMap.put(3L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        for (long i = 7; i < 8; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        }
        for (long i = 9; i < 11; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }

        transactionsGCMap.put(12L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 14L));
        hashMap.put(12L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        for (long i = 16; i < 20; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        }

        var collector = new MVCCGarbageCollector();
        Long maxMinId = collector.findMaxMinId(transactionsGCMap, 18L);
        ConcurrentSkipListSet<Long> activeOrEmptyTransactionsIds =
                collector.findMissingOrActiveTransactionsIds(maxMinId, 18L, transactionsGCMap);

        ConcurrentSkipListSet<Long> correctActiveOrEmptyTransactionsIds =
                new ConcurrentSkipListSet<>(Arrays.asList(7L, 8L, 11L, 15L, 16L, 17L));

        Assert.assertEquals(correctActiveOrEmptyTransactionsIds, activeOrEmptyTransactionsIds);
    }
}
