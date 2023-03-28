package jetbrains.exodus.entitystore;

import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.newLogConcept.GarbageCollector.MVCCGarbageCollector;
import jetbrains.exodus.newLogConcept.GarbageCollector.TransactionGCEntry;
import jetbrains.exodus.newLogConcept.MVCC.MVCCDataStructure;
import jetbrains.exodus.newLogConcept.MVCC.MVCCRecord;
import jetbrains.exodus.newLogConcept.Transaction.Transaction;
import jetbrains.exodus.newLogConcept.Transaction.TransactionState;
import org.jctools.maps.NonBlockingHashMapLong;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


public class MVCCGarbageCollectorTest {

    // TODO fixme - unlock collector in MVCCComponent and fix assertion error
    @Test
    public void modifyValueWith12Threads1_000_100NewTransactionsOnEachIncrementTest() throws ExecutionException, InterruptedException {
        ExecutorService service = Executors.newCachedThreadPool();
        Map<String, String> keyValTransactions = new HashMap<>();
        var mvccComponent = new MVCCDataStructure();

        String keyString = "key-" + (int) (Math.random() * 100000);
        AtomicLong value = new AtomicLong(1000);
        keyValTransactions.put(keyString, String.valueOf(value));

        for (int i = 0; i < 120; i++) {
            for (int j = 0; j < 1_000_000; j++) {
                var th = service.submit(() -> {
                    Transaction writeTransaction = mvccComponent.startWriteTransaction();
                    // check record is null before the commit
                    mvccComponent.put(writeTransaction, StringBinding.stringToEntry(keyString),
                            StringBinding.stringToEntry(String.valueOf(value)));

                    Assert.assertEquals(writeTransaction.getSnapshotId(), writeTransaction.getOperationLinkList().get(0).getTxId());
                    mvccComponent.commitTransaction(writeTransaction);
                });
                th.get();
            }
            value.getAndIncrement();
        }
        Assert.assertEquals(value.get(), 1120);
    }


    @Test
    public void testCleanWithAllCommittedOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 7; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(7L, hashMap, transactionsGCMap);

        Assert.assertTrue(hashMap.containsKey(6L));

        for (long i = 1; i < 6; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }
    }

    @Test
    public void testCleanWithMissingIdsOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }
        for (long i = 6; i < 7; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(10L, hashMap, transactionsGCMap);

        Assert.assertTrue(hashMap.containsKey(3L));
        Assert.assertTrue(hashMap.containsKey(6L));

        for (long i = 1; i < 3; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }
        for (long i = 4; i < 6; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }

    }

    @Test
    public void testCleanWithMergedIdsOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }
        for (long i = 4; i < 7; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        TransactionGCEntry entry = new TransactionGCEntry(TransactionState.COMMITTED.get(), 6);
        transactionsGCMap.put(4L, entry);

        for (long i = 7; i < 8; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(10L, hashMap, transactionsGCMap);
        for (long i = 1; i < 7; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(7L));
    }

    @Test
    public void testCleanWithMergedIdsAndMissedValueOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        }

        for (long i = 5; i < 6; i++){
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        TransactionGCEntry entry = new TransactionGCEntry(TransactionState.COMMITTED.get(), 6);
        transactionsGCMap.put(5L, entry);
        for (long i = 7; i < 8; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(10L, hashMap, transactionsGCMap);

        Assert.assertFalse(hashMap.containsKey(1L));
        Assert.assertFalse(hashMap.containsKey(2L));
        Assert.assertTrue(hashMap.containsKey(3L));
        Assert.assertFalse(hashMap.containsKey(4L));
        Assert.assertTrue(hashMap.containsKey(5L));
        Assert.assertFalse(hashMap.containsKey(6L));
        Assert.assertFalse(hashMap.containsKey(7L));

        Assert.assertEquals(collector.findMaxMinId(transactionsGCMap, 10L).longValue(), 3L);
    }


    @Test
    public void testCleanWithNoMaxMinId() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        transactionsGCMap.put(5L, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        transactionsGCMap.put(6L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 7L));
        hashMap.put(5L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        hashMap.put(6L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        for (long i = 8; i < 9; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(10L, hashMap, transactionsGCMap);

        Assert.assertTrue(hashMap.containsKey(5L));
        Assert.assertTrue(hashMap.containsKey(6L));
        Assert.assertTrue(hashMap.containsKey(8L));

    }

    @Test
    public void testCleanWithoutMissingIdsOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 4; i < 6; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 6; i < 7; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        var collector = new MVCCGarbageCollector();
        collector.clean(10L, hashMap, transactionsGCMap);

        Assert.assertFalse(hashMap.containsKey(1L));
        Assert.assertFalse(hashMap.containsKey(2L));
        for (long i = 3; i < 7; i++){
            Assert.assertTrue(hashMap.containsKey(i));
        }
    }

    @Test
    public void testCleanWithRevertedIdsOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 4; i < 6; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.REVERTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 6; i < 7; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        var collector = new MVCCGarbageCollector();
        collector.clean(10L, hashMap, transactionsGCMap);

        for (long i = 1; i < 6; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(6L));
    }


    @Test
    public void testCleanWithActiveTransactionsIdsOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 4; i < 7; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        for (long i = 7; i < 10; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(15L, hashMap, transactionsGCMap);

        for (long i = 1; i < 3; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }
        for (long i = 4; i < 7; i++){
            Assert.assertTrue(hashMap.containsKey(i));
        }
        for (long i = 8; i < 10; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(7L));
    }

    @Test
    public void getCleanWithMissingIdsOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 5; i < 6; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.REVERTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        for (long i = 10; i < 11; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(11L, hashMap, transactionsGCMap);

        for (long i = 1; i < 3; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(3L));
        Assert.assertTrue(hashMap.containsKey(5L));
        Assert.assertTrue(hashMap.containsKey(10L));
    }

    @Test
    public void testCleanMissingTransactionsIdsOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 5; i < 6; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.REVERTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 6; i < 8; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 9; i < 11; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        for (long i = 13; i < 20; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(15L, hashMap, transactionsGCMap);

        for (long i = 1; i < 3; i++){
            Assert.assertFalse(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(3L));
        for (long i = 5; i < 8; i++){
            Assert.assertTrue(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(9L));
        Assert.assertFalse(hashMap.containsKey(10L));
        for (long i = 13; i < 15; i++){
            Assert.assertTrue(hashMap.containsKey(i));
        }
    }

    @Test
    public void testActiveAndMissingTransactionsIdsWithUpToOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        transactionsGCMap.put(1L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 2L));
        hashMap.put(1L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        transactionsGCMap.put(3L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 6L));
        hashMap.put(3L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        for (long i = 7; i < 8; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 9; i < 11; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        transactionsGCMap.put(12L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 14L));
        hashMap.put(12L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        for (long i = 16; i < 20; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(18L, hashMap, transactionsGCMap);

        Assert.assertFalse(hashMap.containsKey(1L));
        Assert.assertTrue(hashMap.containsKey(3L));
        Assert.assertTrue(hashMap.containsKey(7L));
        Assert.assertTrue(hashMap.containsKey(9L));
        Assert.assertFalse(hashMap.containsKey(10L));
        Assert.assertTrue(hashMap.containsKey(12L));
        for (long i = 16; i < 18; i++){
            Assert.assertTrue(hashMap.containsKey(i));
        }
    }


    @Test
    public void testCleanWithNoMaxMinIdOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys
        for (long i = 1; i < 7; i++) {
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        hashMap.put(9L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        transactionsGCMap.put(1L, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        transactionsGCMap.put(2L, new TransactionGCEntry(TransactionState.REVERTED.get()));
        transactionsGCMap.put(3L, new TransactionGCEntry(TransactionState.COMMITTED.get()));
        transactionsGCMap.put(4L, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        transactionsGCMap.put(5L, new TransactionGCEntry(TransactionState.REVERTED.get()));
        transactionsGCMap.put(6L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 8L));
        transactionsGCMap.put(9L, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));

        var collector = new MVCCGarbageCollector();
        collector.clean(10L, hashMap, transactionsGCMap);

        for (long i = 1; i < 7; i++) {
            Assert.assertTrue(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(9L));
    }

    @Test
    public void testCleanWithNoMaxMinIdOneThread2() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 6; i++) {
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        transactionsGCMap.put(1L, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        transactionsGCMap.put(2L, new TransactionGCEntry(TransactionState.REVERTED.get()));
        transactionsGCMap.put(3L, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        transactionsGCMap.put(4L, new TransactionGCEntry(TransactionState.REVERTED.get()));
        transactionsGCMap.put(5L, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));

        var collector = new MVCCGarbageCollector();
        collector.clean(10L, hashMap, transactionsGCMap);

        for (long i = 1; i < 6; i++) {
            Assert.assertTrue(hashMap.containsKey(i));
        }
    }

    @Test
    public void testCleanAllCommittedOrRevertedOneThread2() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        for (long i = 1; i < 4; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.REVERTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }
        for (long i = 4; i < 7; i++){
            transactionsGCMap.put(i, new TransactionGCEntry(TransactionState.COMMITTED.get()));
            hashMap.put(i, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        }

        var collector = new MVCCGarbageCollector();
        collector.clean(7L, hashMap, transactionsGCMap);

        for (long i = 1; i < 6; i++) {
            Assert.assertFalse(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(6L));
    }


    @Test
    public void testCleanWithAllCommittedAndMergedOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        transactionsGCMap.put(2L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 4L));
        transactionsGCMap.put(5L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 6L));

        hashMap.put(2L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        hashMap.put(5L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        var collector = new MVCCGarbageCollector();
        collector.clean(7L, hashMap, transactionsGCMap);

        for (long i = 1; i < 5; i++) {
            Assert.assertFalse(hashMap.containsKey(i));
        }
        Assert.assertTrue(hashMap.containsKey(5L));
    }

    @Test
    public void testCleanWithAllMergedOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        transactionsGCMap.put(2L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 7L));
        hashMap.put(2L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        var collector = new MVCCGarbageCollector();
        collector.clean(6L, hashMap, transactionsGCMap);

        Assert.assertTrue(hashMap.containsKey(2L));

        for (long i = 3; i < 6; i++) {
            Assert.assertFalse(hashMap.containsKey(i));
        }
    }

    @Test
    public void testCleanWithAlmostAllMergedOneThread() {
        ConcurrentSkipListMap<Long, TransactionGCEntry> transactionsGCMap = new ConcurrentSkipListMap<>();
        NonBlockingHashMapLong<MVCCRecord> hashMap = new NonBlockingHashMapLong<>(); // primitive long keys

        transactionsGCMap.put(2L, new TransactionGCEntry(TransactionState.COMMITTED.get(), 7L));
        transactionsGCMap.put(8L, new TransactionGCEntry(TransactionState.IN_PROGRESS.get()));
        hashMap.put(2L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));
        hashMap.put(8L, new MVCCRecord(new AtomicLong(0), new ConcurrentLinkedQueue<>()));

        var collector = new MVCCGarbageCollector();
        collector.clean(9L, hashMap, transactionsGCMap);

        Assert.assertTrue(hashMap.containsKey(2L));
        Assert.assertTrue(hashMap.containsKey(8L));

        for (long i = 3; i < 8; i++) {
            Assert.assertFalse(hashMap.containsKey(i));
        }
    }
}
